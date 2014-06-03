#!/usr/bin/env python
'''
    This map-reduce job takes in 3 Terabytes Yume Request Data,
    Output only CA request data with selected features

    Mapper Output Format:
    identifier_deliverypoint  \t  feature_list (remove the parsed ip )
'''
import sys

STATE_IND = 24
COOKIE_IND = 44
DELIVERY_IND = 31

PROFILE_IDX = [
               #0, # session_id - majority
               1, # domain_id - majority
               2, # placement_id
               3, # advertisement_id
               4, # requested_date - frequency within 4 hours
               
               5, # census_DMA - majority

               6, # city_name - jaccard set  

               7,  # publisher_id - set
               8, # content_video_id (skip 0)
               9, # service_provider_id - jaccard set
               10, # key_value - jaccard set
               11, # player_location_id 
               12, # player_size_id - jaccard set
               13, # page_fold_id - majority
               14, # ad_visibility
               15, # ovp_version  
               16, # ovp_type
               
               17, # hid

               18, # is_on_premise
               19, # audience_segment (skip NULL)


               20, # referrer_site (skip NULL)
               21, # network_id - set
               22, # slot_type_id - majority (low weight because too many 1)
               23, # ad_request_id
               24, # is_not_yume_white_list  - ratio of true
               25, # publisher_channel_id - (skip 0)
               26, # content_video_identifier (skip null)
               27, # content_profile_id (skip null)
               28, # is_pre_fetch_request
               29 # service_provider_name  - majority
               
               #30,  # ip_addr - jaccard set
               # 31 # behavior_cookie (skip NULL)
             ];

set_idx = [
               1, # placement_id
               #2, # advertisement_id
               3, # requested_date - frequency within 4 hours
               5, # city_name - jaccard set  
               7, # content_video_id (skip 0)
               14, # ovp_version  
               15, # ovp_type
               16, # hid
               18, # audience_segment (skip NULL)
               19, # referrer_site (skip NULL)
               21, # slot_type_id - majority (low weight because too many 1)
               24, # publisher_channel_id - (skip 0)
               25, # content_video_identifier (skip null)
               26 # content_profile_id (skip null)
               #30,  # ip_addr - jaccard set
               #31 # behavior_cookie (skip NULL)
          ]
          
def get_majority(lst):
    return max(set(lst), key=lst.count)
               
def get_empty_list_of_list(num_elem):
  list_of_list = []
  for i in range(0, num_elem):
    list_of_list.append([])
  return list_of_list

def set_to_string (attr_set):
  return '|'.join(attr_set)


attr_num = len(PROFILE_IDX);
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into multiple records 
    record_list = l[1].split('|')
    
    list_of_list = get_empty_list_of_list(attr_num)
    for record in record_list:
      attr_l = record.split(',')
      attr_list = [attr_l[i] for i in PROFILE_IDX]
      for i in xrange(0, len(attr_list)):
        list_of_list[i].append(attr_list[i])

    # print result
    attr_res = []
    for idx in xrange(attr_num):
      # time; city; hid => as evaluation (index starts from 0)
      if (idx in set_idx):
        attr_res.append (set_to_string(set(list_of_list[idx])))
      else:
        attr_res.append (get_majority(list_of_list[idx]))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))

