#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
import math
from datetime import datetime
import csv
'''

   This map-reduce job is used to emit edges from LSH bucket

   Mapper output format:
        (start_node, end_node) \t profile1 || profile2

   Reducer output format:
        (sart_node, end_node) \t similarity_location_eval (possible dp eval)


'''
THRESHOLD = 0.83
GEO_DIFF_THRESHOLD = 0.03
def read_csv (csv_file):
  data = []
  with open(csv_file, "rb") as f:
    reader = csv.reader(f)
    for row in reader:
      data.append (row)
  return data

def build_geo_map(location_file):
  # build geo location map
  data = read_csv (location_file)
  geo_map = {}
  for record in data[1:]:
    geo_map[record[3]] = (float(record[5]), float(record[6]))
  # print len(geo_map)
  return geo_map

def cal_geo_dist_sqr (city1, city2, geo_map):
  loc1 = geo_map[city1]
  loc2 = geo_map[city2]
  dist_sqr = math.pow((loc1[0] - loc2[0]), 2) + math.pow((loc2[1] - loc2[1]), 2)
  return dist_sqr


def fail_time_loc(time_loc1, time_loc2):
  t1, loc1 = time_loc1.split('|')
  t2, loc2 = time_loc2.split('|')
  time_diff = datetime.strptime(t1, '%Y-%m-%d %H:%M') - datetime.strptime(t2, '%Y-%m-%d %H:%M')
  time_diff = time_diff.seconds / 3600
  # if time difference is too small => error
  if (time_diff < 5):
    return True

  loc_diff = cal_geo_dist_sqr(loc1, loc2, GEO_MAP)
  if (loc_diff / time_diff > GEO_DIFF_THRESHOLD):
    return True
  else:
    return False


def eval_time_location(tuple_list1, tuple_list2):
  for time_loc1 in tuple_list1:
    for time_loc2 in tuple_list2:
      if (fail_time_loc(time_loc1, time_loc2)):
        return False
  return True


def get_time_loc_tuple_list(time_loc_str):
  time_loc_tuple_list = time_loc_str.split('??')


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def make_key(str1, str2):
  if (str1 < str2):
    return (str1, str2)
  else:
    return (str2, str1)

# only allow one bucket mistake
def pass_support_check(support_cnt):
  return support_cnt >= 1


# request_skip_set and beacon_skip_set are the subset of this
skip_list = \
  [   
    1, # placement_id (skip 0)
    2, # advertisement_id (skip 0)
    5, # content_video_id  (skip 0)
    7, # key_value (skip 0_0)
    12, #ovp_version (skip NA)
    13, #ovp_type (skip NA)
    14, #hid (skip 0)
    16, #audience_segment (skip NULL)
    17, # referrer_site (skip NULL)
    22, # publisher_channel_id (skip 0)
    23, # content_profile_id (skipp null)
  ]

skip_value_dict = \
{ 
  1: '0',
  2: '0',
  5: '0',
  7: '0_0',
  12: 'na',
  13: 'na',
  14: '0',
  16: 'null',
  17: 'null',
  22: '0',
  23: 'null'
}

skip_expectation_dict = \
  {
    1: 1.0/3670, # placement_id
    2: 1.0/1533, #advertsiment_id
    5: 1.0/23, # content_video_id  (skip 0)
    7: 1.0/962, # key_value (skip 0_0)
    12: 1.0/11, #ovp_version (skip NA)
    13: 1.0/11, #ovp_type (skip NA)
    14: 1.0/1452216, #hid (skip 0)
    16: 1.0/16, #audience_segment (skip NULL)
    17: 1.0/17, # referrer_site (skip NULL)
    22: 1.0/344, # publisher_channel_id (skip 0)
    23: 1.0/23, # content_profile_id (skipp null1/231/962 
  }



def cal_jaccard (record1, record2):
    num = 0
    denom = len(record1)    
    comp_list1 = [record1[i] for i in xrange(denom) if i not in skip_list]
    comp_list2 = [record2[i] for i in xrange(denom) if i not in skip_list]
    num = sum([1 for i in xrange(len(comp_list1)) if comp_list1[i] == comp_list2[i]])

    # deal with skip list
    for i in skip_list:
      if (i < denom):
        if (record1[i] == skip_value_dict[i] or record2[i] == skip_value_dict[i]):
          num += skip_expectation_dict[i]
        elif (record1[i] == record2[i]):
          num += 1
    
    return float(num) / denom
 

def cal_cosine(record1, record2):
    cross = 0.0
    norm1 = 0.0
    norm2 = 0.0
    for i in range(len(record1)):
      r1 = float(record1[i])
      r2 = float(record2[i])            
      cross += r1*r2
      norm1 += r1*r1
      norm2 += r2*r2
            
    denom = math.sqrt(norm1) * math.sqrt(norm2)
    if denom != 0:
        #print cross / denom
        return cross / denom
    else:
        return 0.0

# exclude identifier and evaluation field
BEC_ONLY = 24
REQ_ONLY = 26
RB_UNION = 33

BEC_CAT = 18
RB_UNION_CAT = 27
RB_COMMON = 17


# is that ok to use expecation value to calculate similarity for all other undefined value ???
def cal_similarity(profile1, profile2):
     # last attribute is identifier, and the second by last is the evaluation metric
     x_list = profile1.split(',')[:-2]
     y_list = profile2.split(',')[:-2]

     score = 0.0
     if len(x_list) > len(y_list):
         #swap x_list and y_list, so len(x) <= len(y)
         tmp = x_list
         x_list = y_list
         y_list = tmp
     if len(x_list) == len(y_list):
         if len(x_list) == REQ_ONLY:
             score = cal_jaccard(x_list, y_list)
         elif len(x_list) == BEC_ONLY:
             request1 = [x_list[i] for i in xrange(BEC_CAT)]
             request2 = [y_list[i] for i in xrange(BEC_CAT)]
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[BEC_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         else:
             request1 = [x_list[i] for i in xrange(RB_UNION_CAT)]
             request2 = [y_list[i] for i in xrange(RB_UNION_CAT)]
             beacon1 = x_list[RB_UNION_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.8 * cal_jaccard(request1, request2) + 0.2 * cal_cosine(beacon1, beacon2)
     else:
         if len(x_list) == BEC_ONLY and len(y_list) == REQ_ONLY:
             request1 = [x_list[i] for i in xrange(RB_COMMON)]
             request2 = [y_list[i] for i in xrange(RB_COMMON)]
             score = cal_jaccard(request1, request2)
         elif len(x_list) == BEC_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in xrange(BEC_CAT)]
             request2 = [y_list[i] for i in xrange(RB_COMMON)]
             request2.append(y_list[REQ_ONLY])
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         elif len(x_list) == REQ_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in range(REQ_ONLY)]
             request2 = [y_list[i] for i in range(REQ_ONLY)]
             score = cal_jaccard(request1, request2)
     return score



GEO_MAP = build_geo_map ('US-City-Location.csv')
# concat version
def main(separator='\t'):
    
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        support_cnt = 0
        device1 = None; device2 = None;
        for pair, profiles in group:
          if (support_cnt == 0):
            device1, device2 = profiles.split('||')
          support_cnt += 1
        
        key1 = device1.split(',')[-1]
        key2 = device2.split(',')[-1]
        if (pass_support_check(support_cnt)):
          sim = cal_similarity(device1, device2)
          if (sim > THRESHOLD):
            key = make_key(key1, key2)
            time_loc_str1 = device1.split(',')[-2]
            time_loc_tuple_list1 = time_loc_str1.split('??')
            time_loc_str2 = device2.split(',')[-2]
            time_loc_tuple_list2 = time_loc_str2.split('??')

            eval_flag = eval_time_location(time_loc_tuple_list1, time_loc_tuple_list2)
            eval_flag = '_' + str(eval_flag)
            print ("%s%s%.6f%s" %(key, '\t', sim, eval_flag))
      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "Reducer ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

