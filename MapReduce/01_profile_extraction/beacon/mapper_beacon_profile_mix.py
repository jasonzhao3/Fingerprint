#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

CITY_IND = 5
TIME_IND = 3
HID_IND = 17
NUM_IND = 21
PROFILE_IDX = [   
    1, # domain id      
    2, # placement_id
    3, # advertisement_id
    4, # event time
    5, # census_dma_id
    6, # city
    7, # publisher_channel_id
    8, # content_video_id
    9, # delivery point
    10, # service provider id
    11, # key values 
    12, # player location 
    13, # player size
    14, # page fold
    15, # ad visibility
    16, # ovp version
    17, # ovp type
    18, # hid
    19, # is on-premise
    20, # audience segments
    ############################
    21, # slate id
    ############################     
    22, # zero_tracker
    23, # twentry_five
    24, # fifty
    25, # seventry_five
    26, # one_hundred
    27 # volume percent
]


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
               14 # ad_visibility
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
               20  # slate id
          ]


attr_num = len(PROFILE_IDX)
# input comes from STDIN (standard input)
for line in sys.stdin:
      line = line.strip()
      identifier, value = line.split('\t')
      #print(identifier)
      # split the line into words
      record_list = value.split('|')
      attr_count = []
      for i in range(0, attr_num):
        attr_count.append([])
      for record in record_list:
        attr_l = record.split(',')
        attr_list = [attr_l[i] for i in PROFILE_IDX]
        for i in range(0, len(attr_list)):
            attr_count[i].append(attr_list[i])
      string_res = '';
 
      for i in range(0, len(attr_count)):
              attrs = attr_count[i]
              if i in set_idx:               
                  #union set
                  m = set()
                  for attr in attrs:
                    m.add(attr)
                  string_res += '|'.join(m)
              elif i in majority_idx:
                  # majority
                  m = dict()
                  for attr in attrs:
                    if attr not in m:
                        m[attr]  = 0
                    m[attr] += 1
                  maxNum = 0
                  maxItem = ''
                  for key,value in m.items():
                    if (value > maxNum):
                        maxNum = value;
                        maxItem = key
                  string_res += maxItem
                  #print string_res
              else:
                  # numerical
                  denom = len(attrs)
                  num = 0
                  for attr in attrs:
                      num += int(attr)
                  string_res += str(float(num)/denom)
              if i != len(attr_count)-1:
                  string_res += ','
      print '%s%s%s' % (identifier, "\t", string_res)

