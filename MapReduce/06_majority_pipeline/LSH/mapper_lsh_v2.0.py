#!/usr/bin/env python

import sys

'''
   This map-reduce job is used to LSH profiles into multiple buckets

   Mapper output format:
        bucket_idx_group \t attribute_list

  Corresponding reducer: identical reducer

'''


import csv
from time import strptime, mktime
import numpy as np
import math


# total number of devices: 2800m
HASH_STRING_CONST_MOD = 20000000
# NUM_BUCKET = [item * 1000000 for item in xrange(5,21)]
# bucket_suffix =  ['5m', '6m', '7m', '8m', '9m', '10m', '11m', '12m', '13m',
#               '14m', '15m', '16m', '17m', '18m', '19m', '20m']
NUM_BUCKET = [12000000]
bucket_suffix = ['12m']

'''
  device_profile: a list of attributes
  - profile may be a joined profile of request and beacon (length: 38)
  - or just request (length: 31)
  - or just beacon (length: 27)
'''
def hash_majority (device_profile):
  if (len(device_profile) == 38):
    return hash_full_profile (device_profile)
  elif (len(device_profile) == 31):
    return hash_request_profile (device_profile)
  elif (len(device_profile) == 27):
    return hash_beacon_profile (device_profile)
  else:
    pass

'''
  full profile index 
'''

full_int_idx = [
               0, # domain_id - majority
               1, # placement_id
               2, # advertisement_id
               4, # census_DMA - majority
               6,  # publisher_id - set
               7, # content_video_id (skip 0)
               8, # delivery_point_id
               9, # service_provider_id - jaccard set
               11, # player_location_id 
               12, # player_size_id - jaccard set
               13, # page_fold_id - majority
               14, # ad_visibility
               18, # is_on_premise

               21, # network_id - set
               22, # slot_type_id - majority (low weight because too many 1)
               23, # ad_request_id
               
               25, # publisher_channel_id - (skip 0)
               26, # content_video_identifier (skip null)
               27, # content_profile_id (skip null)

               ##############
               31 # slate_id
]

full_float_idx = [
           32, #zero_tracker
           33, # twentry_five
           34, # fifty
           35, # seventry_five
           36, # one_hundred
           37 # volume percent
]


full_string_idx = [
        20, # referrer_site (skip NULL)
        29, # service_provider_name  - majority
        30, # behavior_cookie (skip NULL)
]

full_boolean_idx = [
        24, # is_not_yume_white_list  - ratio of true
        28, # is_pre_fetch_request
]

#10, # key_value - jaccard set
#15, # ovp_version  
#16, # ovp_type
#19, # audience_segment => need specially handled



'''
  request profile index
'''
request_int_idx = [
         0, # domain_id - majority
               1, # placement_id
               2, # advertisement_id
               4, # census_DMA - majority
               6,  # publisher_id - set
               7, # content_video_id (skip 0)
               8, # delivery_point_id
               9, # service_provider_id - jaccard set
               11, # player_location_id 
               12, # player_size_id - jaccard set
               13, # page_fold_id - majority
               14, # ad_visibility
               18, # is_on_premise

               21, # network_id - set
               22, # slot_type_id - majority (low weight because too many 1)
               23, # ad_request_id
               
               25, # publisher_channel_id - (skip 0)
               26, # content_video_identifier (skip null)
               27, # content_profile_id (skip null)
]

request_string_idx = [
        20, # referrer_site (skip NULL)
        29, # service_provider_name  - majority
        30, # behavior_cookie (skip NULL)
]

request_boolean_idx = [
        24, # is_not_yume_white_list  - ratio of true
        28, # is_pre_fetch_request
]

#10, # key_value - jaccard set
#15, # ovp_version  
#16, # ovp_type
#19, # audience_segment => need specially handled



'''
  beacon profile index
'''
beacon_int_idx = [
               0, # domain_id - majority
               1, # placement_id
               2, # advertisement_id
               4, # census_DMA - majority
               6,  # publisher_id - set
               7, # content_video_id (skip 0)
               8, # delivery_point_id
               9, # service_provider_id - jaccard set
               11, # player_location_id 
               12, # player_size_id - jaccard set
               13, # page_fold_id - majority
               14, # ad_visibility
               18, # is_on_premise

               20, # slate_id
]

beacon_float_idx = [
           21, # zero_tracker
           22, # twentry_five
           23, # fifty
           24, # seventry_five
           25, # one_hundred
           26  # volume percent
]


#10, # key_value - jaccard set
#15, # ovp_version  
#16, # ovp_type
#19, # audience_segment => need specially handled


# evaluation
# 3, # requested_date
# 5, # city
# 17, # hid

def hash_string (input_str):
    djb2_code = 5381
    for i in xrange (0, len (input_str)):
        char = input_str[i];
        djb2_code = (djb2_code << 5) + djb2_code + ord (char)
    return djb2_code % HASH_STRING_CONST_MOD

def sum_int (device_profile, index_list):
  int_list = [(idx+1) * int(device_profile[idx]) for idx in index_list if device_profile[idx].isdigit()]
  return sum(int_list)

def sum_float (device_profile, index_list):
  float_list = [(idx+1) * float(device_profile[idx]) for idx in index_list]
  return 1000 * sum(float_list)

def sum_string (device_profile, index_list):
  string_list = [device_profile[idx] for idx in index_list]
  hash_val = 0.0
  for string in string_list:
    hash_val += hash_string (string)
  return hash_val

def sum_boolean (device_profile, index_list):
  boolean_list = [device_profile[idx] for idx in index_list]
  hash_val = 0.0
  for boolean_string in boolean_list:
    hash_val += hash_string (boolean_string)
  return hash_val

def hash_key_val (key_val_str):
  try:
    key, val = key_val_str.split('_')
    return 33 * int(key) + 73 * int(val)
  except (RuntimeError, TypeError, NameError, ValueError):
    return 0

def hash_ovp (ovp_str):
  try:
    int_list = ovp_str.split('.')
    int_list = [int(item) * int(item) for item in int_list]
    return sum(int_list)
  except (RuntimeError, TypeError, NameError, ValueError):
    return 0

def hash_audience_segment (audience_segment_str):
  if (audience_segment_str.isdigit()):
    return int(audience_segment_str)
  else:
    try:
      int_list = audience_segment_str.split(';')
      int_list = [int(item) for item in int_list]
      return sum(int_list)
    except (RuntimeError, TypeError, NameError, ValueError):
      return 0


'''
  example device profile:
  01HnsUspFXjd5R  1737,266055,125441,
          2014-04-10 19:12:16.53|2014-04-10 19:11:48.962|
          2014-04-10 19:12:06.965|2014-04-10 19:11:08.634|
          2014-04-10 19:11:32.378|2014-04-10 19:11:22.718|
          2014-04-10 19:11:56.767,4,Laguna Niguel,682,0,1,
          0,0_0,2,2,1,1,NA,NA,1076403343,1,NULL,techbrowsing.com,
          135,1,1,false,0,null,null,false,Alentus Internet Services,
          NULL,0,0.2,0.2,0.2,0.2,0.2,0.0
'''

def hash_full_profile (device_profile):
  hash_val = sum_int (device_profile, full_int_idx)
  hash_val += sum_float (device_profile, full_float_idx)
  hash_val += sum_string (device_profile, full_string_idx)
  hash_val += sum_boolean (device_profile, full_boolean_idx)

  hash_val += hash_key_val (device_profile[10])
  hash_val += hash_ovp (device_profile[15])
  hash_val += hash_ovp (device_profile[16])
  hash_val += hash_audience_segment (device_profile[19])

  bucket_list = [int(hash_val) % den for den in NUM_BUCKET]
  bucket_list = [str(bucket_list[i])+'_'+ bucket_suffix[i] for i in xrange(len(bucket_list))]
  return bucket_list




'''
  Example device profile:
  01I8osS3dles9F  7509,260047,122459,
  2014-04-11 14:24:05.642,4,
  Diamond Bar,1138,0,2,65,0_0,1,0,0,1,NA,NA,
  1113031201,1,NULL,s0.2mdn.net,1,1,1,false,0,
  null,null,false,Sprint PCS,NULL
'''
def hash_request_profile (device_profile):
  hash_val = sum_int (device_profile, request_int_idx)
  hash_val += sum_string (device_profile, request_string_idx)
  hash_val += sum_boolean (device_profile, request_boolean_idx)

  hash_val += hash_key_val (device_profile[10])
  hash_val += hash_ovp (device_profile[15])
  hash_val += hash_ovp (device_profile[16])
  hash_val += hash_audience_segment (device_profile[19])

  bucket_list = [int(hash_val) % den for den in NUM_BUCKET]
  bucket_list = [str(bucket_list[i])+'_'+ bucket_suffix[i] for i in xrange(len(bucket_list))]
  return bucket_list


'''
  Example device profile:
'''
def hash_beacon_profile (device_profile):
  hash_val = sum_int (device_profile, beacon_int_idx)
  hash_val += sum_float (device_profile, beacon_float_idx)

  hash_val += hash_key_val (device_profile[10])
  hash_val += hash_ovp (device_profile[15])
  hash_val += hash_ovp (device_profile[16])
  hash_val += hash_audience_segment (device_profile[19])

  bucket_list = [int(hash_val) % den for den in NUM_BUCKET]
  bucket_list = [str(bucket_list[i])+'_'+ bucket_suffix[i] for i in xrange(len(bucket_list))]
  return bucket_list









# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    bucket_list = hash_majority (attr_list)
    attr_list.append(l[0])
    for bucket in bucket_list:
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

