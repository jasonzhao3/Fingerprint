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
#     8, "service_provider_id",
#     9, "key_value",                             remove 0_0
#     10, "player_location", 
#     11, "player_size",
#     12, "page_fold",
#     13, "ad_visibility",
#     14, "ovp_version",                            remove NA
#     15, "ovp_type",                             remove NA
#     16, "hid",                                    remove 0
#     17, "is_on_premise"
#     18, "audience_segment",                       remove null
# ############################
#     19, "referrer_site",                      remove null
#     20, "network_id",
#     21, "slot_type_id", 
#     22, "ad_request_id",
#     23, "is_not_yume_white_list",  
#     24, "publisher_channel_id",                remove 0
#     #25, "content_video_identifier",            remove null
#     26, "content_profile_id",                   remove null
#     27, "is_pre_fetch_request",
#     #28, "service_provider_name",
# ############################
#     29, "slate_id"                                remove 0
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
      print '%s%s%s' % (identifier, "\t", attr_list[16])
      

      