#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

# full_profile = [
#                0, # domain_id - majority
#                1, # placement_id
#                2, # advertisement_id
#                3, # census_DMA - majority
#                4,  # publisher_id - set (skip 0)
#                5, # content_video_id (skip 0)
#                6, # service_provider_id - jaccard set
#                7, # key_value - jaccard set
#                8, # player_location_id 
#                9, # player_size_id - jaccard set
#                10, # page_fold_id - majority
#                11, # ad_visibility
#                12, # ovp_version  
#                13, # ovp_type
#                14, # hid
#                15, # is_on_premise
#                16, # audience_segment (skip NULL)
#                 ############
#                17, # referrer_site (skip NULL)
#                18, # network_id - set
#                19, # slot_type_id - majority (low weight because too many 1)
#                20, # ad_request_id
#                21, # is_not_yume_white_list  - ratio of true
#                22, # publisher_channel_id - (skip 0)
#                23, # content_profile_id (skip null)
#                24, # is_pre_fetch_request
#                25, # service_provider_name  - majority         
             

#             26, # slate id
#             ############################     
#             27, # zero_tracker
#             28, # twentry_five
#             29, # fifty
#             30, # seventry_five
#             31, # one_hundred
#             32, # volume percent
#             33, # time_location tuple
# ]

FULL_PROFILE_LEN = 34
BEACON_PROFILE_LEN = 25
REQUEST_PROFILE_LEN = 27


BEC_CAT = 18
RB_UNION_CAT = 27
RB_COMMON = 17


#skip attributes
PUBLISHER_IDX = 4
CONTENT_VIDEO_IDX = 5
KEY_VALUE_IDX = 7


# input comes from STDIN (standard input)
for line in sys.stdin:
      line = line.strip()
      identifier, profile = line.split('\t')
      #print(identifier)
      # split the line into words
      attr_list = profile.split(',')

      if len(attr_list) == BEACON_PROFILE_LEN:
        for i in range(0,BEC_CAT):
            for value in attr_list[i].split('|'):
              if i < RB_COMMON:
                if len(value) == 0 or value.lower() == "null" or value.lower() == "na" or \
                ((i == 4 or i == 5) and value == "0") or (i == 7 and value == "0_0"):
                  continue
                print '%s%s%s' % (i, "\t", value)
              else:
                print '%s%s%s' % (i+9, "\t", value)
      if len(attr_list) == REQUEST_PROFILE_LEN or len(attr_list) == FULL_PROFILE_LEN:
        for i in range(0,len(attr_list)):
          if i < REQUEST_PROFILE_LEN and i != len(attr_list) - 1:
            for value in attr_list[i].split('|'):
              if len(value) == 0 or value.lower() == "null" or value.lower() == "na" or \
                ((i == 4 or i == 5 or i == 22) and value == "0") or (i == 7 and value == "0_0"):
                  continue
              print '%s%s%s' % (i, "\t", value)
      

      