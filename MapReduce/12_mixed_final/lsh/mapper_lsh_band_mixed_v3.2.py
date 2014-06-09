#!/usr/bin/env python

import sys
import hashlib
from random import randint
import random
import json
'''

Step 1:
   This map-reduce job is used to LSH profiles into multiple buckets and bands

   Mapper output format:
        bucket_idx_band \t profile

  Change get_signature function 



'''
HID_IDX = 14
CONTENT_VIDEO_IDX = 5
PUBLISHER_IDX = 22
NUMERICAL_START = 27
OVP_IDX = 12
OVP_TYPE_IDX = 13
# include time_location evaluation
FULL_PROFILE_LEN = 34
BEACON_PROFILE_LEN = 25
REQUEST_PROFILE_LEN = 27

majority_idx = [
               0, # domain_id - majority
               2, # advertisement_id
               3, # census_DMA - majority
               4,  # publisher_id - majority
               6, # service_provider_id - majority
               #7, # key_value
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               11, # ad_visibility
               15, # is_on_premise
               18, # network_id
               20, # ad_request_count
               21, # is_not_yume_white_list  - ratio of true
               24, # is_pre_fetch_request
               25, # service_provider_name  - majority
]

set_idx = [
               1, # placement_id
               5, # content_video_id (skip 0)
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               19, # slot_type_id - majority (low weight because too many 1)
               22, # publisher_channel_id - (skip 0)
               #25, # content_video_identifier (skip null)
               23 # content_profile_id (skip null)
               #26 # slate_id
]


# altogether 6 bands, and the last band should be hashed numerically
full_band_idx = \
{ 
    #   'full_majority':
    # [
    #    0, # domain_id - majority
    #    #1, # placement_id
    #    2, # advertisement_id
    #    3, # census_DMA - majority
    #    4,  # publisher_id - set
    #    # 5, # content_video_id (skip 0)
    #    6, # service_provider_id - jaccard set
    #    # 7, # key_value - jaccard set
    #    8, # player_location_id 
    #    9, # player_size_id - jaccard set
    #    10, # page_fold_id - majority
    #    11, # ad_visibility
    #    12, # ovp_version  
    #    13, # ovp_type
    #    14, # hid (skip 0)
    #    15, # is_on_premise
    #    # 16, # audience_segment (skip NULL)
    #     ############
    #    # 17, # referrer_site (skip NULL)
    #    18, # network_id - set
    #    #19, # slot_type_id - majority (low weight because too many 1)
    #    20, # ad_request_id
    #    21, # is_not_yume_white_list  - ratio of true
    #    #22, # publisher_channel_id - (skip 0)
    #    #23, # content_profile_id (skip null)
    #    24, # is_pre_fetch_request
    #    25, # service_provider_name  - majority
    #    27, # zero_tracker
    #     28, # twentry_five
    #     29, # fifty
    #     30, # seventry_five
    #     31, # one_hundred
    #     32 # volume percent
    #   ],

      'common':
      [
       0, # domain_id - majority
       1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority  (weight 3)
       # 4,  # publisher_id - set
       # # 5, # content_video_id (skip 0)
       # 6, # service_provider_id - jaccard set
       # # 7, # key_value - jaccard set
       # 8, # player_location_id 
       # 9, # player_size_id - jaccard set
       # 10, # page_fold_id - majority
       # 11, # ad_visibility
       # 12, # ovp_version  
       # 13, # ovp_type
       14, # hid (weight 5)
       # 15, # is_on_premise
       # # 16, # audience_segment (skip NULL)
      ],


      # 'full_set':
      # [
      #  #0, # domain_id - majority
      #  1, # placement_id
      #  #2, # advertisement_id
      #  3, # census_DMA - majority
      #  4,  # publisher_id - set
      #  5, # content_video_id (skip 0)
      #  12, # ovp_version  
      #  13, # ovp_type
      #  14, # hid
      #  #15, # is_on_premise
      #  # 16, # audience_segment (skip NULL)
      #  18, # network_id - set
      #  19, # slot_type_id - majority (low weight because too many 1)
      #  #20, # ad_request_id
      #  #21, # is_not_yume_white_list  - ratio of true
      #  22, # publisher_channel_id - (skip 0)
      #  23 # content_profile_id (skip null)
       
      # ],

      # 'beacon_specific':
      # [
      #          3, # census_DMA - majority
      #          8, # player_location_id 
      #          9, # player_size_id - jaccard set
      #          10, # page_fold_id - majority
      #          12, # ovp_version  
      #          13, # ovp_type
      #          14, # hid
      #          #26,
      #   27, # zero_tracker
      #   28, # twentry_five
      #   29, # fifty
      #   30, # seventry_five
      #   31, # one_hundred
      #   32 # volume percent
      # ],


      # 'request_specific':
      # [        
      #          3, # census_DMA - majority
      #          8, # player_location_id 
      #          9, # player_size_id - jaccard set
      #          10, # page_fold_id - majority
      #          12, # ovp_version  
      #          13, # ovp_type
      #          14, # hid
      #          18, # network_id - set
      #          19, # slot_type_id - majority (low weight because too many 1)
      #          20, # ad_request_count
      #          21, # is_not_yume_white_list  - ratio of true
      #          22, # publisher_channel_id - (skip 0)
      #          #25, # content_video_identifier (skip null)
      #          23, # content_profile_id (skip null)
      #          24, # is_pre_fetch_request
      #          25 # service_provider_name  - majority
      # ]


}


# altogether 2 bands
'''
  request band index
'''
request_band_idx = \
{ 
       'common':
      [
       0, # domain_id - majority
       1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority  (weight 3)
       # 4,  # publisher_id - set
       # # 5, # content_video_id (skip 0)
       # 6, # service_provider_id - jaccard set
       # # 7, # key_value - jaccard set
       # 8, # player_location_id 
       # 9, # player_size_id - jaccard set
       # 10, # page_fold_id - majority
       # 11, # ad_visibility
       # 12, # ovp_version  
       # 13, # ovp_type
       14, # hid (weight 5)
       # 15, # is_on_premise
       # # 16, # audience_segment (skip NULL)
      ],
      
# 'request_specific':
#       [        
#                3, # census_DMA - majority
#                8, # player_location_id 
#                9, # player_size_id - jaccard set
#                10, # page_fold_id - majority
#                12, # ovp_version  
#                13, # ovp_type
#                14, # hid
#                18, # network_id - set
#                19, # slot_type_id - majority (low weight because too many 1)
#                20, # ad_request_count
#                21, # is_not_yume_white_list  - ratio of true
#                22, # publisher_channel_id - (skip 0)
#                #25, # content_video_identifier (skip null)
#                23, # content_profile_id (skip null)
#                24, # is_pre_fetch_request
#                25 # service_provider_name  - majority
#       ]


}


# altogether 2 bands, last band is numerical
beacon_band_idx = \
{
       'common':
      [
       0, # domain_id - majority
       1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority  (weight 3)
       # 4,  # publisher_id - set
       # # 5, # content_video_id (skip 0)
       # 6, # service_provider_id - jaccard set
       # # 7, # key_value - jaccard set
       # 8, # player_location_id 
       # 9, # player_size_id - jaccard set
       # 10, # page_fold_id - majority
       # 11, # ad_visibility
       # 12, # ovp_version  
       # 13, # ovp_type
       14, # hid (weight 5)
       # 15, # is_on_premise
       # # 16, # audience_segment (skip NULL)
      ],


      # 'beacon_specific':
      # [
      #          3, # census_DMA - majority
      #          8, # player_location_id 
      #          9, # player_size_id - jaccard set
      #          10, # page_fold_id - majority
      #          12, # ovp_version  
      #          13, # ovp_type
      #          14, # hid
      #          #26,
      #   18, # zero_tracker
      #   19, # twentry_five
      #   20, # fifty
      #   21, # seventry_five
      #   22, # one_hundred
      #   23 # volume percent
      # ]

}



# evaluation
# 3, # requested_date
# 5, # city
# 16, # hid

def get_interval(tracker):
  if float(tracker) < 0.25:
    return "1"
  elif float(tracker) < 0.5:
    return "2"
  elif float(tracker) < 0.75:
    return "3"
  else: 
    return "4"



'''
  hash set - min hash

  search_map_list: a list of seach_map
  - each search_map: key: attribute_idx   val: search_dictionary
  - inside(search_dictionary)  key: attribute_value   val: permutation_idx
'''

def get_signature (feature_set, search_map_list, index): 
    signatures = []
    # 3 search_map => 3 signatures
    for search_map in search_map_list:
      search_dict = search_map[str(index)]
      index_list = []
      # for each feature value, find its index => finally find the min_idx
      for feature in feature_set:
        try:
          index = search_dict[feature]
        except (KeyError):
          index = randint(0, len(search_dict)-1)
        index_list.append(index)
      min_idx = min(index_list)
      signatures.append(min_idx)

    signatures = [str(item) for item in signatures]
    return ','.join(signatures)



def fnv32a( str ):
    hval = 0x811c9dc5
    fnv_32_prime = 0x01000193
    uint32_max = 2 ** 32
    for s in str:
        hval = hval ^ ord(s)
        hval = (hval * fnv_32_prime) % uint32_max
    return hval 

def sha256(str):
  return int(hashlib.sha256(str).hexdigest(), 16)

def reform_profile(device_profile):
  if (len(device_profile) == FULL_PROFILE_LEN):
    for idx in xrange(27,33):
      device_profile[idx] = get_interval(device_profile[idx])
  elif (len(device_profile) == BEACON_PROFILE_LEN):
    for idx in xrange(18,24):
      device_profile[idx] = get_interval(device_profile[idx])
  return device_profile


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
  device_profile = reform_profile(device_profile)
  bucket_list = []
  for key, band_idx_list in full_band_idx.iteritems():
    band_attrs = []
    for idx in band_idx_list:
      if idx in majority_idx:
        band_attrs.append(device_profile[idx])
      elif idx < NUMERICAL_START:
        band_attrs.append(get_signature(device_profile[idx].split('|'), search_map_list, idx))
      else:
        band_attrs.append(get_interval(device_profile[idx]))
    band_attrs_str = ','.join(band_attrs)
    hash_val = sha256(band_attrs_str)
    bucket_list.append(str(hash_val) + '_' + key)
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
  bucket_list = []
  for key, band_idx_list in request_band_idx.iteritems():
    band_attrs = []
    for idx in band_idx_list:
      if idx in majority_idx:
        band_attrs.append(device_profile[idx])
      elif idx in set_idx:
        band_attrs.append(get_signature(device_profile[idx].split('|'), search_map_list, idx))
    band_attrs_str = ','.join(band_attrs)
    hash_val = sha256(band_attrs_str)
    bucket_list.append(str(hash_val) + '_' + key)
  return bucket_list 

'''
  Example device profile:
'''

def hash_beacon_profile (device_profile):
  bucket_list = []
  for key, band_idx_list in beacon_band_idx.iteritems():
    band_attrs = []
    for idx in band_idx_list:
      if idx in majority_idx:
        band_attrs.append(device_profile[idx])
      elif idx in set_idx and idx < 18:
          band_attrs.append(get_signature(device_profile[idx].split('|'), search_map_list, idx))
      else:
        band_attrs.append(get_interval(device_profile[idx]))
    band_attrs_str = ','.join(band_attrs)
    hash_val = sha256(band_attrs_str)
    bucket_list.append(str(hash_val) + '_' + key)
  return bucket_list 

'''
  device_profile: a list of attributes
  - profile may be a joined profile of request and beacon (length: 36)
  - or just request (length: 29) -- remove deliverypoint and behavior_cookie
  - or just beacon (length: 26)  -- remove deliverypoint
'''
def hash_mixed (device_profile):
  if (len(device_profile) == FULL_PROFILE_LEN):
    return hash_full_profile (device_profile)
  elif (len(device_profile) == REQUEST_PROFILE_LEN):
    return hash_request_profile (device_profile)
  elif (len(device_profile) == BEACON_PROFILE_LEN):
    return hash_beacon_profile (device_profile)
  else:
    print "error", len(device_profile)
    pass



with open ('perm_json.json', 'r') as json_data:
  search_map_list = json.load(json_data)

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    bucket_list = hash_mixed (attr_list)
    attr_list.append(l[0])
    for bucket in bucket_list:
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

