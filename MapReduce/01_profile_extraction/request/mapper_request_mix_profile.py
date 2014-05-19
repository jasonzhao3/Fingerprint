#!/usr/bin/env python
'''
    This map-reduce job takes in request record data,
    and output majority-based profile of each device

    Mapper Output Format:
    identifier  \t  majority-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

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
               9, # delivery_point_id
               10, # service_provider_id - jaccard set
               11, # key_value - jaccard set
               12, # player_location_id 
               13, # player_size_id - jaccard set
               14, # page_fold_id - majority
               15, # ad_visibility
               16, # ovp_version  
               17, # ovp_type
               
               18, # hid

               19, # is_on_premise
               20, # audience_segment (skip NULL)


               21, # referrer_site (skip NULL)
               22, # network_id - set
               23, # slot_type_id - majority (low weight because too many 1)
               24, # ad_request_id
               25, # is_not_yume_white_list  - ratio of true
               26, # publisher_channel_id - (skip 0)
               27, # content_video_identifier (skip null)
               28, # content_profile_id (skip null)
               29, # is_pre_fetch_request
               30, # service_provider_name  - majority
               
               #31,  # ip_addr - jaccard set
               
               32 # behavior_cookie (skip NULL)
             ];


majority_idx = [
               0, # domain_id - majority
               4, # census_DMA - majority
               6,  # publisher_id - majority
               8, # delivery_point_id
               9, # service_provider_id - jaccard set
               10, # key_value
               11, # player_location_id 
               12, # player_size_id - jaccard set
               13, # page_fold_id - majority
               1, # ad_visibility
               21, # network_id - set
               22, # slot_type_id - majority (low weight because too many 1)
               23, # ad_request_count
               24, # is_not_yume_white_list  - ratio of true
               29, # service_provider_name  - majority
              ]

set_idx = [
               1, # placement_id
               2, # advertisement_id
               3, # requested_date - frequency within 4 hours
               5, # city_name - jaccard set  
               7, # content_video_id (skip 0)
               15, # ovp_version  
               16, # ovp_type
               17, # hid
               18, # is_on_premise
               19, # audience_segment (skip NULL)
               20, # referrer_site (skip NULL)
               25, # publisher_channel_id - (skip 0)
               26, # content_video_identifier (skip null)
               27, # content_profile_id (skip null)
               28, # is_pre_fetch_request
               #30,  # ip_addr - jaccard set
               31 # behavior_cookie (skip NULL)
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


attr_num = len(PROFILE_IDX)
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
      # time; city; hid => as evaluation
      if (idx in majority_idx):
        attr_res.append (get_majority(list_of_list[idx]))
      else:
        attr_res.append (set_to_string(set(list_of_list[idx])))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))

