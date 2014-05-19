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
              if i == CITY_IND or i == TIME_IND or i == HID_IND:
                  
                  #union set
                  m = set()
                  for attr in attrs:
                    m.add(attr)
                  string_res += '|'.join(m)
              elif i < NUM_IND:
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