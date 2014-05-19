#!/usr/bin/env python
'''
    This map-reduce job takes in request record data,
    and output set-based profile of each device

    Mapper Output Format:
    identifier  \t  set-based feature list 
    e.g. 
    00WEb5zE9xX1ze \t 805,153,2037,false,Carlsbad,29|4,Verizon,32_223;31_392|32_94;31_392,76.176.121.33|70.211.14.65|70.211.9.239|70.211.2.23,3,8,1|0,none,1188236865|1286633761|1188235759|1188233751,1

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

def set_to_string (attr_set):
  return '|'.join(attr_set)

               
def get_empty_list_of_list(num_elem):
  list_of_list = []
  for i in range(0, num_elem):
    list_of_list.append([])
  return list_of_list


attr_num = len(PROFILE_IDX);
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into multiple records
    record_list = l[1].split('|')
    attr_count = []

    # build record attribute list_of_list
    list_of_list = get_empty_list_of_list(attr_num)
    for record in record_list:
      attr_l = record.split(',')
      attr_list = [attr_l[i] for i in PROFILE_IDX]
      for i in xrange(0, len(attr_list)):
        list_of_list[i].append(attr_list[i])

    attr_res = []
    for idx in xrange(attr_num):
      attr_res.append (set_to_string(set(list_of_list[idx])))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))

