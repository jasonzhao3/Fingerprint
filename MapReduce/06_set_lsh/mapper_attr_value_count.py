#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

# ATTR_NAME = [   
#     0, "domain_id",      
#     1, "placement_id",
#     2, "advertisement_id",
#     3, "event_time",
#     4, "census_dma_id",
#     5, "city",
#     6, "publisher_channel_id",                  remove 0
#     7, "content_video_id",                      remove 0
#     8, "delivery_point_id",
#     9, "service_provider_id",
#     10, "key_value",                             remove 0_0
#     11, "player_location", 
#     12, "player_size",
#     13, "page_fold",
#     14, "ad_visibility",
#     15, "ovp_version",                            remove NA
#     16, "ovp_type",                             remove NA
#     17, "hid",                                    remove 0
#     18, "is_on_premise"
#     19, "audience_segment",                       remove null
# ############################
#     20, "referrer_site",                      remove null
#     21, "network_id",
#     22, "slot_type_id", 
#     #23, "ad_request_id",
#     24, "is_not_yume_white_list",  
#     25, "publisher_channel_id",                remove 0
#     #26, "content_video_identifier",            remove null
#     27, "content_profile_id",                   remove null
#     28, "is_pre_fetch_request",
#     #29, "service_provider_name",
#     #30, "behavior_cookie",                      remove NULL
# ############################
#     31, "slate_id"                                remove 0
# ############################     
#     # zero_tracker
#     # twentry_five
#     # fifty
#     # seventry_five
#     # one_hundred
#     # volume percent
# ]

# input comes from STDIN (standard input)
for line in sys.stdin:
      line = line.strip()
      identifier, profile = line.split('\t')
      #print(identifier)
      # split the line into words
      attr_list = profile.split(',')

      if len(attr_list) == 27:
        for i in range(0,21):
          if i != 3 and i != 5 and i != 17 and i != 26 and i != 29 and i != 30:
            for value in attr_list[i].split('|'):
              if i < 20:
                if len(value) == 0 or value.lower() == "null" or value.lower() == "na" or \
                ((i == 6 or i == 7 or i == 25 or i == 31) and value == "0") or (i == 10 and value == "0_0"):
                  continue
                print '%s%s%s' % (i, "\t", value)
              else:
                print '%s%s%s' % (i+11, "\t", value)
      if len(attr_list) == 31 or len(attr_list) == 38:
        for i in range(0,len(attr_list)):
          if i < 32 and i != 3 and i != 5 and i != 17 and i != 26 and i != 29 and i != 30:
            for value in attr_list[i].split('|'):
              if len(value) == 0 or value.lower() == "null" or value.lower() == "na" or \
                ((i == 6 or i == 7 or i == 25 or i == 31) and value == "0") or (i == 10 and value == "0_0"):
                  continue
              print '%s%s%s' % (i, "\t", value)
      

      