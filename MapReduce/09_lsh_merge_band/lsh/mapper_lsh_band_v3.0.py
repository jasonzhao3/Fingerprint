#!/usr/bin/env python

import sys
import hashlib

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



KEY_VAL_IND = 9 #9, # key_value - jaccard set
OVP_VERSION_IND = 14 #14, # ovp_version  
OVP_TYPE_IND = 15 #15, # ovp_type
AUD_SEG_IND = 18 #18, # audience_segment => need special


# altogether 6 bands, and the last band should be hashed numerically
full_band_idx = \
{ 
'full_leave_3id':
    [
       # 0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       # 6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # referrer_site (skip NULL)
       20, # network_id - set
       21, # slot_type_id - majority (low weight because too many 1)
       22, # ad_request_id
       23, # is_not_yume_white_list  - ratio of true
       #########################################
       24, # publisher_channel_id - (skip 0)
       25, # content_video_identifier (skip null)
       # 26, # content_profile_id (skip null)
       27, # is_pre_fetch_request
       28, # service_provider_name  - majority
       29, # slate_id
   
       30, #zero_tracker
       31, # twentry_five
       32, # fifty
       33, # seventry_five
       34, # one_hundred
       35 # volume percent
      ],

  'full_leave_boolean':
    [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       # 17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # referrer_site (skip NULL)
       20, # network_id - set
       21, # slot_type_id - majority (low weight because too many 1)
       22, # ad_request_id
       # 23, # is_not_yume_white_list  - ratio of true
       #########################################
       24, # publisher_channel_id - (skip 0)
       25, # content_video_identifier (skip null)
       26, # content_profile_id (skip null)
       # 27, # is_pre_fetch_request
       28, # service_provider_name  - majority
       29, # slate_id
   
       30, #zero_tracker
       31, # twentry_five
       32, # fifty
       33, # seventry_five
       34, # one_hundred
       35 # volume percent
      ],

    'full_leave_numerical':
    [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # referrer_site (skip NULL)
       20, # network_id - set
       21, # slot_type_id - majority (low weight because too many 1)
       22, # ad_request_id
       23, # is_not_yume_white_list  - ratio of true
       #########################################
       24, # publisher_channel_id - (skip 0)
       25, # content_video_identifier (skip null)
       26, # content_profile_id (skip null)
       27, # is_pre_fetch_request
       28, # service_provider_name  - majority
       29, # slate_id
   
       # 30, #zero_tracker
       31, # twentry_five
       32, # fifty
       # 33, # seventry_five
       34, # one_hundred
       # 35 # volume percent
      ],

      'full_leave_beacon':
    [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # referrer_site (skip NULL)
       20, # network_id - set
       21, # slot_type_id - majority (low weight because too many 1)
       22, # ad_request_id
       23, # is_not_yume_white_list  - ratio of true
       #########################################
       24, # publisher_channel_id - (skip 0)
       25, # content_video_identifier (skip null)
       26, # content_profile_id (skip null)
       27, # is_pre_fetch_request
       28, # service_provider_name  - majority
      ],

      'full_leave_request':
      [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       29, # slate_id
   
       30, #zero_tracker
       31, # twentry_five
       32, # fifty
       33, # seventry_five
       34, # one_hundred
       35 # volume percent
      ],


      'common':
      [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled
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
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled
      ],

       'full_leave_beacon':
    [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # referrer_site (skip NULL)
       20, # network_id - set
       21, # slot_type_id - majority (low weight because too many 1)
       22, # ad_request_id
       23, # is_not_yume_white_list  - ratio of true
       #########################################
       24, # publisher_channel_id - (skip 0)
       25, # content_video_identifier (skip null)
       26, # content_profile_id (skip null)
       27, # is_pre_fetch_request
       28, # service_provider_name  - majority
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
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled
      ],

      'full_leave_request':
       [
       0, # domain_id - majority
       1, # placement_id    
       2, # advertisement_id
       4, # census_DMA - majority
       6,  # publisher_id - set
       7, # content_video_id (skip 0)]
       8, # service_provider_id - jaccard set
       9, # key_value - jaccard set
       10, # player_location_id 
       11, # player_size_id - jaccard set
       12, # page_fold_id - majority
       13, # ad_visibility
       14, # ovp_version  
       15, # ovp_type
       17, # is_on_premise
       18, # audience_segment => need specially handled

       19, # slate_id
   
       20, #zero_tracker
       21, # twentry_five
       22, # fifty
       23, # seventry_five
       24, # one_hundred
       25 # volume percent
      ],
}



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
  bucket_list = []
  for key, band_idx_list in full_band_idx.iteritems():
    band_attrs = [device_profile[i] for i in band_idx_list]
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
    band_attrs = [device_profile[i] for i in band_idx_list]
    band_attrs_str = ','.join(band_attrs)
    hash_val = sha256(band_attrs_str)
    bucket_list.append(str(hash_val) + '_' + key)
  return bucket_list 

'''
  Example device profile:
'''
def hash_beacon_profile (device_profile):
  bucket_list = []
  for key in beacon_band_idx.keys():
    band_idx_list = beacon_band_idx[key]
    band_attrs = [device_profile[i] for i in band_idx_list]
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
    attr_list.append(l[0])
    for bucket in bucket_list:
      print '%s%s%s' % (bucket, "\t", ','.join(attr_list))
    

