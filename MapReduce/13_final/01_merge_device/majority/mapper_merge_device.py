#!/usr/bin/env python

import sys
import hashlib
from random import randint
from operator import itemgetter

'''

Step 1:
   This map-reduce job is used to combine multiple identifiers that belong to the same online/TV device
   For mobile and tablet device, the script just skips them

  input: joined profile
  output: new joined profile after merging 

   Mapper output format:
        hash_bucket \t profile
        e.g. for online/TV, multiple profiles may belong to the same hash_bucket
             for mobile/tablet, unique identifier as hash_bucket
   Reducer output format:
        new_identifier \t profile
        e.g. for online/TV, new_identifier is the concated identifier
        e.g. for mobile/tablet, new_identifier is the origial identifier

'''

# include time_location evaluation, exclude identifier
FULL_PROFILE_LEN = 34
BEACON_PROFILE_LEN = 25
REQUEST_PROFILE_LEN = 27


# altogether 6 bands, and the last band should be hashed numerically
full_profile = \
    [
               0, # domain_id - majority
               1, # placement_id
               # 2, # advertisement_id
               3, # census_DMA - majority
               4,  # publisher_id - set
               # 5, # content_video_id (skip 0)
               6, # service_provider_id - jaccard set
               7, # key_value - jaccard set
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               11, # ad_visibility
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               15, # is_on_premise
               16, # audience_segment (skip NULL)
                ############
               # 17, # referrer_site (skip NULL)
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_id
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               # 23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25, # service_provider_name  - majority         
             

            26, # slate id
            ############################     
            # 27, # zero_tracker
            # 28, # twentry_five
            # 29, # fifty
            # 30, # seventry_five
            # 31, # one_hundred
            # 32, # volume percent
            33, # time_location tuple
      ]


# altogether 2 bands
'''
  request band index
'''
request_profile = \
    [
        0, # domain_id - majority
               1, # placement_id
               # 2, # advertisement_id
               3, # census_DMA - majority
               4,  # publisher_id - set
               # 5, # content_video_id (skip 0)
               6, # service_provider_id - jaccard set
               7, # key_value - jaccard set
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               11, # ad_visibility
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               15, # is_on_premise
               16, # audience_segment (skip NULL)
                ############
               # 17, # referrer_site (skip NULL)
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_id
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               # 23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25, # service_provider_name  - majority         
               26, # time_location tuple
      ]



# altogether 2 bands, last band is numerical
beacon_profile = \
      [
         0, # domain id      
    1, # placement_id
    # 2, # advertisement_id
    3, # census_dma_id
    4, # publisher_channel_id
    # 5, # content_video_id
    6, # service provider id
    7, # key values 
    8, # player location 
    9, # player size
    10, # page fold
    11, # ad visibility
    12, # ovp version
    13, # ovp type
    14, # hid
    15, # is on-premise
    16, # audience segments
    ############################
    17, # slate id
    ############################     
    # 18, # zero_tracker
    # 19, # twentry_five
    # 20, # fifty
    # 21, # seventry_five
    # 22, # one_hundred
    # 23, # volume percent
    24, # time_location tuple
      ]



# evaluation
# 3, # requested_date
# 5, # city
# 16, # hid

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


def get_majority_city(tuple_list_str):
  tuple_list = tuple_list_str.split('??')
  city_counter = dict()
  for time_loc in tuple_list:
    time, loc = time_loc.split('|')
    if (loc not in city_counter):
      city_counter[loc] = 1
    else:
      city_counter[loc] += 1
  return max(city_counter.iteritems(), key=itemgetter(1))[0]



def reform_profile(device_profile):
  device_profile[-1] = get_majority_city(device_profile[-1])
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
  hash_attrs = [device_profile[i] for i in full_profile]
  hash_attrs_str = ','.join(hash_attrs)
  hash_val = sha256(hash_attrs_str)
  return str(hash_val) + '_' + 'full_profile'


'''
  Example device profile:
  01I8osS3dles9F  7509,260047,122459,
  2014-04-11 14:24:05.642,4,
  Diamond Bar,1138,0,2,65,0_0,1,0,0,1,NA,NA,
  1113031201,1,NULL,s0.2mdn.net,1,1,1,false,0,
  null,null,false,Sprint PCS,NULL
'''
def hash_request_profile (device_profile):
  device_profile = reform_profile(device_profile)
  hash_attrs = [device_profile[i] for i in request_profile]
  hash_attrs_str = ','.join(hash_attrs)
  hash_val = sha256(hash_attrs_str)
  return str(hash_val) + '_' + 'request_profile'


'''
  Example device profile:
'''
def hash_beacon_profile (device_profile):
  device_profile = reform_profile(device_profile)
  hash_attrs = [device_profile[i] for i in beacon_profile]
  hash_attrs_str = ','.join(hash_attrs)
  hash_val = sha256(hash_attrs_str)
  return str(hash_val) + '_' + 'beacon_profile'



'''
  device_profile: a list of attributes
  - profile may be a joined profile of request and beacon (length: 36)
  - or just request (length: 29) -- remove deliverypoint and behavior_cookie
  - or just beacon (length: 26)  -- remove deliverypoint
'''
def hash_majority (device_profile):
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
    delivery_point = l[0].split('_')[-1]

    # skip mobile/tablet
    if (delivery_point == '2' or delivery_point == '3'):
      print '%s' % line
    # online/tv
    else:
      attr_list = l[1].split(',')
      bucket = hash_majority (attr_list)
      attr_list.append(l[0])
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

