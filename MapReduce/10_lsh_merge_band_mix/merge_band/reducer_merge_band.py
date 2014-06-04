#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
import math
'''

Step 1:
   This map-reduce job is used to LSH profiles into multiple buckets and bands

   Mapper output format:
        bucket_idx_band \t profile, identifier

  Corresponding reducer: identical reducer

Step 2:
  Mapper: Cn2 => output (device1, device2) \t 1
  Reducer: count support, remove lows support and low similarity pairs  => edge list

Another parallel step:
  output all identifiers (nodes)

TODO: the two steps can be concated eventually, and the intermediate output is on HDFS rather than s3

'''

FULL_LENGTH = 36  # 7 bands
REQUEST_LENGTH = 29 # 5 bands
BEACON_LENGTH = 26 # 5 bands



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


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def make_key(str1, str2):
  if (str1 < str2):
    return (str1, str2)
  else:
    return (str2, str1)

# only allow one bucket mistake
def pass_support_check(device1, device2, support_cnt):
  return support_cnt >= 1

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




# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        support_cnt = 0
        device1 = "null"; device2 = "null";
        for pair, profiles in group:
          if (support_cnt == 0):
            device1, device2 = profiles.split('||')
          support_cnt += 1
        
        device1 = device1.split(',')
        device2 = device2.split(',')
        key1 = device1[-1]
        key2 = device2[-1]
        if (pass_support_check(device1[:-1], device2[:-1], support_cnt)):
          sim = cal_similarity(device1, device2)
          if (sim > 0.2):
            key = make_key(key1, key2)
            print ("%s%s%.6f" %(key, '\t', sim))
      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

