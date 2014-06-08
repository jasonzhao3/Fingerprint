#!/usr/bin/env python

import sys
import hashlib
from random import randint
import random
'''

Step 1:
   This map-reduce job is used to LSH profiles into multiple buckets and bands

   Mapper output format:
        bucket_idx_band \t profile

  Corresponding reducer: concat profiles within each bucket_idx_band with separator ||

Step 2:
  Mapper: Cn2 => output (device1, device2) \t 1
  Reducer: count support, remove lows support and low similarity pairs  => edge list

Another parallel step:
  output all identifiers (nodes)

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
               #18, # audience_segment (skip NULL)
               #19, # referrer_site (skip NULL)
               19, # slot_type_id - majority (low weight because too many 1)
               22, # publisher_channel_id - (skip 0)
               #25, # content_video_identifier (skip null)
               23 # content_profile_id (skip null)
               #26 # slate_id
]


# altogether 6 bands, and the last band should be hashed numerically
full_band_idx = \
{ 
      'full_majority':
    [
       0, # domain_id - majority
       #1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority
       4,  # publisher_id - set
       # 5, # content_video_id (skip 0)
       6, # service_provider_id - jaccard set
       # 7, # key_value - jaccard set
       8, # player_location_id 
       9, # player_size_id - jaccard set
       10, # page_fold_id - majority
       11, # ad_visibility
       12, # ovp_version  
       13, # ovp_type
       14, # hid (skip 0)
       15, # is_on_premise
       # 16, # audience_segment (skip NULL)
        ############
       # 17, # referrer_site (skip NULL)
       18, # network_id - set
       #19, # slot_type_id - majority (low weight because too many 1)
       20, # ad_request_id
       21, # is_not_yume_white_list  - ratio of true
       #22, # publisher_channel_id - (skip 0)
       #23, # content_profile_id (skip null)
       24, # is_pre_fetch_request
       25, # service_provider_name  - majority
       27, # zero_tracker
        28, # twentry_five
        29, # fifty
        30, # seventry_five
        31, # one_hundred
        32 # volume percent
      ],

      'common':
      [
       0, # domain_id - majority
       1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority
       4,  # publisher_id - set
       5, # content_video_id (skip 0)
       6, # service_provider_id - jaccard set
       # 7, # key_value - jaccard set
       8, # player_location_id 
       9, # player_size_id - jaccard set
       10, # page_fold_id - majority
       11, # ad_visibility
       12, # ovp_version  
       13, # ovp_type
       14, # hid
       15 # is_on_premise
      ],


      'full_set':
      [
       #0, # domain_id - majority
       1, # placement_id
       #2, # advertisement_id
       3, # census_DMA - majority
       4,  # publisher_id - set
       5, # content_video_id (skip 0)
       12, # ovp_version  
       13, # ovp_type
       14, # hid
       #15, # is_on_premise
       # 16, # audience_segment (skip NULL)
       18, # network_id - set
       19, # slot_type_id - majority (low weight because too many 1)
       #20, # ad_request_id
       #21, # is_not_yume_white_list  - ratio of true
       22, # publisher_channel_id - (skip 0)
       23 # content_profile_id (skip null)
       
      ],

      'beacon_specific':
      [
               3, # census_DMA - majority
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               #26,
        27, # zero_tracker
        28, # twentry_five
        29, # fifty
        30, # seventry_five
        31, # one_hundred
        32 # volume percent
      ],


      'request_specific':
      [        
               3, # census_DMA - majority
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_count
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               #25, # content_video_identifier (skip null)
               23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25 # service_provider_name  - majority
      ]


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
       3, # census_DMA - majority
       4,  # publisher_id - set
       5, # content_video_id (skip 0)
       6, # service_provider_id - jaccard set
       # 7, # key_value - jaccard set
       8, # player_location_id 
       9, # player_size_id - jaccard set
       10, # page_fold_id - majority
       11, # ad_visibility
       12, # ovp_version  
       13, # ovp_type
       14, # hid
       15 # is_on_premise
      ],
      
'request_specific':
      [        
               3, # census_DMA - majority
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_count
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               #25, # content_video_identifier (skip null)
               23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25 # service_provider_name  - majority
      ]


}


# altogether 2 bands, last band is numerical
beacon_band_idx = \
{
      'common':
      [
        0, # domain_id - majority
       1, # placement_id
       2, # advertisement_id
       3, # census_DMA - majority
       4,  # publisher_id - set
       5, # content_video_id (skip 0)
       6, # service_provider_id - jaccard set
       # 7, # key_value - jaccard set
       8, # player_location_id 
       9, # player_size_id - jaccard set
       10, # page_fold_id - majority
       11, # ad_visibility
       12, # ovp_version  
       13, # ovp_type
       14, # hid
       15 # is_on_premise
      ],


      'beacon_specific':
      [
               3, # census_DMA - majority
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               #26,
        18, # zero_tracker
        19, # twentry_five
        20, # fifty
        21, # seventry_five
        22, # one_hundred
        23 # volume percent
      ]

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
  build map
'''
def buildValueMap(data_file):
  # build possible values map
  value_map = {}
  ins = open(data_file, "r")
  for line in ins:
      line = line.rstrip()
      key, value = line.split('\t')
      value_list = value.split(',')
      value_map[int(key)] = value_list
  return value_map

def buildPermMap(data_file):
  perm_map = {}
  ins = open(data_file, "r")
  for line in ins:
      line = line.rstrip()
      key, value = line.split('\t')
      str_list = value.split(',')
      perm_list = [int(str_list[i]) for i in range(len(str_list))]
      perm_map[int(key)] = perm_list
  return perm_map
    
value_map = buildValueMap ('attr_count_output')
perm_map = buildPermMap('permutation_output')

hid_map = {}
for i in range(len(value_map[14])):
  hid_map[value_map[14][i]] = perm_map[14][i]


'''
  hash set - min hash
'''
def getSignature (feature_set, value_map, perm_map, index): 
    #print index    
    full_set = value_map[index]
    perm = perm_map[index]
    comb_map = {}
    if (index) == 14: #hid
        if feature_set[0] == "0":
            return str(random.randint(1, len(perm)))
        else:
            return str(min(hid_map[feature] for feature in feature_set))
    for i in range(len(perm)):
        comb_map[perm[i]] = i 
    
    signature = 0
    if len(feature_set) == 1:        
        if (feature_set[0] == "0" and (index == 4 or index == 5 or index == 22)) or feature_set[0].lower() == "null" or feature_set[0].lower() == "na" or feature_set[0].lower() == "n/a":
            signature = random.randint(0, len(perm))
            return str(signature)    


    while signature < len(perm):
        val = full_set[comb_map[signature]]
        if val in feature_set:
            return str(signature)
        else:
            signature += 1
    return str(signature)
    

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
    for i in band_idx_list:
      if i in majority_idx:
        band_attrs.append(device_profile[i])
      elif i < NUMERICAL_START:
        band_attrs.append(getSignature(device_profile[i].split('|'), value_map, perm_map, i))
      else:
        band_attrs.append(get_interval(device_profile[i]))
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
    for i in band_idx_list:
      if i in majority_idx:
        band_attrs.append(device_profile[i])
      elif i in set_idx:
        band_attrs.append(getSignature(device_profile[i].split('|'), value_map, perm_map, i))
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
    for i in band_idx_list:
      if i in majority_idx:
        band_attrs.append(device_profile[i])
      elif i in set_idx and i < 18:
          band_attrs.append(getSignature(device_profile[i].split('|'), value_map, perm_map, i))
      else:
        band_attrs.append(get_interval(device_profile[i]))
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


# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    bucket_list = hash_mixed (attr_list)
    attr_list.append(l[0])
    for bucket in bucket_list:
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

