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
NUM_BUCKET = [30000000]
bucket_suffix = ['30m']


KEY_VAL_IND = 9 #9, # key_value - jaccard set
OVP_VERSION_IND = 14 #14, # ovp_version  
OVP_TYPE_IND = 15 #15, # ovp_type
AUD_SEG_IND = 18 #18, # audience_segment => need special
MSB = 7

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
               #8, # delivery_point_id
               8, # service_provider_id - jaccard set
               10, # player_location_id 
               11, # player_size_id - jaccard set
               12, # page_fold_id - majority
               13, # ad_visibility
               17, # is_on_premise

               #########original full string idx########
               19, # referrer_site (skip NULL)
               28, # service_provider_name  - majority
               #########################################

               20, # network_id - set
               21, # slot_type_id - majority (low weight because too many 1)
               22, # ad_request_id
               
               24, # publisher_channel_id - (skip 0)
               25, # content_video_identifier (skip null)
               26, # content_profile_id (skip null)

               ##############
               29, # slate_id


               ########original full boolean idx #########
               23, # is_not_yume_white_list  - ratio of true
               27, # is_pre_fetch_request
              ################################

               #########original other idx ##########
               9, # key_value - jaccard set
               14, # ovp_version  
               15, # ovp_type
               18 # audience_segment => need specially handled
               #############

]

full_float_idx = [
           30, #zero_tracker
           31, # twentry_five
           32, # fifty
           33, # seventry_five
           34, # one_hundred
           35 # volume percent
]




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
               #8, # delivery_point_id
               8, # service_provider_id - jaccard set
               10, # player_location_id 
               11, # player_size_id - jaccard set
               12, # page_fold_id - majority
               13, # ad_visibility
               17, # is_on_premise

               20, # network_id - set
               21, # slot_type_id - majority (low weight because too many 1)
               22, # ad_request_id
               
               24, # publisher_channel_id - (skip 0)
               25, # content_video_identifier (skip null)
               26, # content_profile_id (skip null)

                #########original full string idx########
               19, # referrer_site (skip NULL)
               28, # service_provider_name  - majority
               #########################################

                 ########original full boolean idx #########
               23, # is_not_yume_white_list  - ratio of true
               27, # is_pre_fetch_request
              ################################

               #########original other idx ##########
               9, # key_value - jaccard set
               14, # ovp_version  
               15, # ovp_type
               18 # audience_segment => need specially handled
               #############
]



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
               # 8, # delivery_point_id
               8, # service_provider_id - jaccard set
               10, # player_location_id 
               11, # player_size_id - jaccard set
               12, # page_fold_id - majority
               13, # ad_visibility
               17, # is_on_premise

               19, # slate_id
               #########original other idx ##########
               9, # key_value - jaccard set
               14, # ovp_version  
               15, # ovp_type
               18 # audience_segment => need specially handled
               #############
]

beacon_float_idx = [
           20, # zero_tracker
           21, # twentry_five
           22, # fifty
           23, # seventry_five
           24, # one_hundred
           25  # volume percent
]



# evaluation
# 3, # requested_date
# 5, # city
# 16, # hid

'''
    get 7 MSB of hashed value
'''

def getMSB(integer):
    if integer < (10 ** MSB):
        return integer

    while integer > (10 ** MSB):
        integer = integer / 10
    return int(integer)
    

def fnv32a( str ):
    hval = 0x811c9dc5
    fnv_32_prime = 0x01000193
    uint32_max = 2 ** 32
    for s in str:
        hval = hval ^ ord(s)
        hval = (hval * fnv_32_prime) % uint32_max
    return hval 

def getHash(string):
    str_list = string.split(',')
    sum = 0
    for s in str_list:
        sum += fnv32a(s)
    return getMSB(int(sum/len(str_list)))

def sum_float (device_profile, index_list):
  float_list = [(idx+1) * float(device_profile[idx]) for idx in index_list]
  return 1000 * sum(float_list)


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
  categorical_attrs = [device_profile[i] for i in full_int_idx]
  categorical_str = ','.join(categorical_attrs)
  hash_val = getHash(categorical_str)
  hash_val += sum_float (device_profile, full_float_idx)
  
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
  categorical_attrs = [device_profile[i] for i in request_int_idx]
  categorical_str = ','.join(categorical_attrs)
  hash_val = getMSB(categorical_str)

  bucket_list = [int(hash_val) % den for den in NUM_BUCKET]
  bucket_list = [str(bucket_list[i])+'_'+ bucket_suffix[i] for i in xrange(len(bucket_list))]
  return bucket_list



'''
  Example device profile:
'''
def hash_beacon_profile (device_profile):
  categorical_attrs = [device_profile[i] for i in beacon_int_idx]
  categorical_str = ','.join(categorical_attrs)
  hash_val = getMSB(categorical_str)

  hash_val += sum_float (device_profile, beacon_float_idx)

  bucket_list = [int(hash_val) % den for den in NUM_BUCKET]
  bucket_list = [str(bucket_list[i])+'_'+ bucket_suffix[i] for i in xrange(len(bucket_list))]
  return bucket_list



'''
  device_profile: a list of attributes
  - profile may be a joined profile of request and beacon (length: 36)
  - or just request (length: 29) -- remove deliverypoint and behavior_cookie
  - or just beacon (length: 26)  -- remove deliverypoint
'''
def hash_majority (device_profile):
  if (len(device_profile) == 36):
    return hash_full_profile (device_profile)
  elif (len(device_profile) == 29):
    return hash_request_profile (device_profile)
  elif (len(device_profile) == 26):
    return hash_beacon_profile (device_profile)
  else:
    print "error", len(device_profile)
    pass





# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    bucket_list = hash_majority (attr_list)
    print bucket_list
    attr_list.append(l[0])
    for bucket in bucket_list:
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

