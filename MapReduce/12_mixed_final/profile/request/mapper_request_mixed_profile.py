#!/usr/bin/env python
'''
    This map-reduce job takes in 3 Terabytes Yume Request Data,
    Output only CA request data with selected features

    Mapper Output Format:
    identifier_deliverypoint  \t  feature_list (remove the parsed ip )
'''
import sys
CITY_IND = 5
TIME_IND = 3
STATE_IND = 24
COOKIE_IND = 44
CONTENT_VIDEO_IND = 25
DELIVERY_IND = 31
NUM_RECORD_LIMIT = 500

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
             ];

set_idx = [
               1, # placement_id
               #2, # advertisement_id
               #3, # requested_date - frequency within 4 hours
               #5, # city_name - jaccard set  
               7, # content_video_id (skip 0)
               14, # ovp_version  
               15, # ovp_type
               16, # hid
               18, # audience_segment (skip NULL)
               19, # referrer_site (skip NULL)
               21, # slot_type_id - majority (low weight because too many 1)
               24, # publisher_channel_id - (skip 0)
               #25, # content_video_identifier (skip null)
               26 # content_profile_id (skip null)
               #30,  # ip_addr - jaccard set
               #31 # behavior_cookie (skip NULL)
          ]

# skip list is lowercase => correspond profile_idx with one offset
skip_list = [
    None, # domain id      
    None, # placement_id
    None, # advertisement_id
    None, # event time
    None, # census_dma_id
    None, # city
    '0', # publisher_channel_id
    '0', # content_video_id
    None, # service provider id
    '0_0', # key values 
    None, # player location 
    None, # player size
    None, # page fold
    None, # ad visibility
    'na', # ovp version
    'na', # ovp type
    '0', # hid
    None, # is on-premise
    'null', # audience segments
    ############################
    'null', # referrer_site (skip NULL)
     None, # network_id - set
     None, # slot_type_id - majority (low weight because too many 1)
     None, # ad_request_id
     None, # is_not_yume_white_list  - ratio of true
     '0', # publisher_channel_id - (skip 0)
     None, # content_video_identifier -- dummy None here
     'null', # content_profile_id (skip null)
     None, # is_pre_fetch_request
     None, # service_provider_name  - majority
]

          
def get_majority(lst, skip_val):
    lst_set = set(lst)
    if (skip_val == None):
        return max(lst_set, key=lst.count)
    else:
        if (max(lst_set, key=lst.count).lower() == skip_val):
            tmp_set = set()
            tmp_set.add(skip_val)
            lst_set -= tmp_set
        if (len(lst_set) == 0):
            return skip_val
        else:
            return max(lst_set, key=lst.count)
               
def get_empty_list_of_list(num_elem):
  list_of_list = []
  for i in range(0, num_elem):
    list_of_list.append([])
  return list_of_list

def set_to_string (attr_set):
  return '|'.join(attr_set)

def comb_time_location(list_of_list):
    time_list = list_of_list[TIME_IND]
    city_list = list_of_list[CITY_IND]
    comb_list = zip(time_list, city_list)
    list_of_list.append(comb_list)
  
def tuple_list_to_str(tuple_list):
    str_list = [item[0][:-7] + '|' + item[1] for item in tuple_list]
    return "??".join(str_list)


attr_num = len(PROFILE_IDX);
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into multiple records 
    record_list = l[1].split('|')
    if (len(record_list) > NUM_RECORD_LIMIT):
      continue
    list_of_list = get_empty_list_of_list(attr_num)
    for record in record_list:
      attr_l = record.split(',')
      attr_list = [attr_l[i] for i in PROFILE_IDX]
      for i in xrange(0, len(attr_list)):
        list_of_list[i].append(attr_list[i])
    
    comb_time_location(list_of_list)
    
    attr_res = []
    for idx in xrange(attr_num):
      # time; city; hid => as evaluation (index starts from 0)
      if (idx == TIME_IND or idx == CITY_IND or idx == CONTENT_VIDEO_IND):
        continue
      elif (idx == len(list_of_list) - 1):
        attr_res.append (tuple_list_to_str(list_of_list[idx]))
      elif (idx in set_idx):
        attr_res.append (set_to_string(set(list_of_list[idx])))
      else:
        attr_res.append (get_majority(list_of_list[idx], skip_list[idx]))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))

