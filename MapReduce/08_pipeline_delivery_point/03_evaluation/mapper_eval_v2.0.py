#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter
import sys, csv, math, os
import random
sys.path.append("./")


'''
  This script is used to see the distribution of devices in each bucket

  Mapper: 
  ========
  input:
  bucket_idx_group \t device

  output:
  user_type__group \t correct or wrong_similarity
  
  => Note: here we group num_of_device into following range:
  [1, 2, 3, 4, 5, 6, 7, 8+]

  Reducer:
  ========
  input:
  user_type__group \t correct or wrong_similarity
  

  output:
  user_type__group \t user_num_____error_ratio_______correct_similarity____error_similarity
  
'''

CITY_IND = 5
TIME_IND = 3
HID_IND = 16

REQ_ONLY = 29
BEC_ONLY = 26
RB_UNION = 36
RB_COMMON = 19

BEC_CAT = 20


RB_UNION_CAT = 30
IS_PREMISE = 17
IS_PREFETCH = 27
THRESHOLD = 0.893


TS_IDX = 3
CITY_IDX = 5
HID_IDX = 16
geo_threshold = 0.08

'''
  Helper functions
'''
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

def cut_to_set(ts_list):
  ts_list = [ts[:-7] for ts in ts_list]
  return set(ts_list)

def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if ((record1[i].lower() != "null" or record2[i].lower() != "null") 
            and (record1[i].lower() != "n/a" or record2[i].lower() != "n/a") 
            and (record1[i].lower() != "na" or record2[i].lower() != "na")): 
            if i != IS_PREFETCH and i != IS_PREMISE:
                if record1[i] != '0' and record2[i] != '0': 
                    denom = denom + 1
                    if record1[i] == record2[i]:
                        num = num + 1 
            else:
                denom = denom + 1
                if record1[i] == record2[i]:
                    num = num + 1   
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

def cal_similarity(dev1, dev2):
  x_list = dev1[:-1]
  y_list = dev2[:-1]
  score = 0.0
  if len(x_list) > len(y_list):
     #swap x_list and y_list, so len(x) <= len(y)
     tmp = x_list
     x_list = y_list
     y_list = tmp
  if len(x_list) == len(y_list):
     if len(x_list) == REQ_ONLY:
         request1 = [x_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         score = cal_jaccard(request1, request2)
     elif len(x_list) == BEC_ONLY:
         request1 = [x_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         beacon1 = x_list[BEC_CAT:]
         beacon2 = y_list[BEC_CAT:]
         score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
     else:
         request1 = [x_list[i] for i in range(RB_UNION_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(RB_UNION_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         beacon1 = x_list[RB_UNION_CAT:]
         beacon2 = y_list[RB_UNION_CAT:]
         score = 0.8 * cal_jaccard(request1, request2) + 0.2 * cal_cosine(beacon1, beacon2)
  else:
     if len(x_list) == BEC_ONLY and len(y_list) == REQ_ONLY:
         request1 = [x_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         score = cal_jaccard(request1, request2)
     elif len(x_list) == BEC_ONLY and len(y_list) == RB_UNION:
         request1 = [x_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2.append(y_list[REQ_ONLY])
         beacon1 = x_list[BEC_CAT:]
         beacon2 = y_list[RB_UNION_CAT:]
         score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
     elif len(x_list) == REQ_ONLY and len(y_list) == RB_UNION:
         request1 = [x_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         request2 = [y_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
         score = cal_jaccard(request1, request2)
  return score
     


# # if half of the pairs are more than 25 miles away, fail
# def fail_location(dev1, dev2):
#   city_list1 = dev1[CITY_IDX].split('|')
#   city_list2 = dev2[CITY_IDX].split('|')

#   total_cnt = len(city_list1) * len(city_list2)
#   if (total_cnt > 1000):
#      city_list1 = random_sample_30 (city_list1)
#      city_list2 = random_sample_30 (city_list2)
#   total_cnt = len(city_list1) * len(city_list2)
  
#   fail_cnt = 0
#   for city1 in city_list1:
#     for city2 in city_list2:
#       try:
#         dist = cal_geo_dist_sqr(city1, city2, geo_map)
#         if (dist > geo_threshold):
#           fail_cnt += 1
#         if (fail_cnt / total_cnt > 0.5):
#           return True
#       except (RuntimeError, TypeError, NameError, KeyError, IOError):
#           pass
#   return False

#if half of the pairs are more than 25 miles away, fail
def fail_location(dev1, dev2):
  city_list1 = dev1[CITY_IDX].split('|')
  city_list2 = dev2[CITY_IDX].split('|')

  city1_set = set(city_list1)
  city2_set = set(city_list2)

  if (len(city1_set & city2_set) == 0):
    return True
  else:
    return False

# as long as they share 50% of their timestamp, fail
def fail_timestamp(dev1, dev2):
  ts1 = dev1[TS_IDX].split('|')
  ts2 = dev2[TS_IDX].split('|')
  ts1_set = cut_to_set(ts1)
  ts2_set = cut_to_set(ts2)
  
  inter_set = ts1_set & ts2_set
  num = len(inter_set)
  den = len(ts1_set) + len(ts2_set)
  if (num / den > 0.5):
    return True
  else:
    return False

# as long as they have too few hid in common, fail
def fail_hid(dev1, dev2):
  hid1 = set(dev1[HID_IDX].split('|'))
  hid2 = set(dev2[HID_IDX].split('|'))
  
  inter_set = hid1 & hid2
  num = len(inter_set)
  den = len(hid1) + len(hid2)
  if (num / den < 0.2):
    return True
  else:
    return False



# success return True, otherwise return False
def eval_cluster(cluster):
  cluster = random_select_cluster(cluster)
  for idx1, dev1 in enumerate(cluster):
    for idx2 in xrange(idx1+1, len(cluster)):
      dev2 = cluster[idx2]
      # if (fail_location(dev1, dev2) or 
      #     fail_timestamp(dev1, dev2) or
      #     fail_hid(dev1, dev2)):
      #   return False
      if (fail_location(dev1, dev2)):
        return False
  return True

def swap (cluster, from_pos, to_pos):
  tmp = cluster[to_pos]
  cluster[to_pos] = cluster[from_pos]
  cluster[from_pos] = tmp


def random_sample_30 (cluster):
  tot_len = len(cluster)
  curr = 0
  while (curr < 30):
    seed = random.randint(curr, tot_len-1)
    swap(cluster, seed, curr)
    curr += 1
  return cluster[:30]
    

def random_select_cluster(cluster):
  total_cnt = len(cluster) * (len(cluster)-1) / 2
  # avoid big cluster
  if (total_cnt > 1000):
    cluster = random_sample_30(cluster)
  
  return cluster



def get_average_similarity(cluster):
  sim = 0.0
  cluster = random_select_cluster(cluster)
  total_cnt = len(cluster) * (len(cluster)-1) / 2

  for idx1, dev1 in enumerate(cluster):
    for idx2 in xrange(idx1+1, len(cluster)):
      dev2 = cluster[idx2]
      sim += cal_similarity(dev1, dev2)
  
  if (total_cnt == 0):
    return 1.0
  return sim / total_cnt


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 




'''
  build geo map
'''
geo_map = build_geo_map ('US-City-Location.csv')
# geo_map = build_geo_map ('../US-City-Location.csv')

        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        num_device_within_bucket = 0
        cluster = []
        for key, device in group:
            num_device_within_bucket += 1
            cluster.append (device.split(','))

        if (num_device_within_bucket > 8):
          num_device_within_bucket = 8
        

        is_correct = eval_cluster(cluster)
        avg_sim = get_average_similarity(cluster)
        group = key.split('_')[1]
        
        key = str(num_device_within_bucket) + '_' + group
 
        if (is_correct):
          print ("%s%s%s%.6f" % (key, '\t', 'correct_', avg_sim))
        else:
          print ("%s%s%s%.6f" % (key, '\t', 'error_', avg_sim))    

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()
