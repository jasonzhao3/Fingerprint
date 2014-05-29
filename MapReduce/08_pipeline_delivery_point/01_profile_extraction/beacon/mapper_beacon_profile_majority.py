#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier_deliverypoint  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

CITY_IND = 5
TIME_IND = 3
HID_IND = 16
NUM_IND = 20  # number of majority features
PROFILE_IDX = [   
    1, # domain id      
    2, # placement_id
    3, # advertisement_id
    4, # event time
    5, # census_dma_id
    6, # city
    7, # publisher_channel_id
    8, # content_video_id
    # 9, # delivery point
    9, # service provider id
    10, # key values 
    11, # player location 
    12, # player size
    13, # page fold
    14, # ad visibility
    15, # ovp version
    16, # ovp type
    17, # hid
    18, # is on-premise
    19, # audience segments
    ############################
    20, # slate id
    ############################     
    21, # zero_tracker
    22, # twentry_five
    23, # fifty
    24, # seventry_five
    25, # one_hundred
    26 # volume percent
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
    line = line.strip()
    l = line.split('\t')
    record_list = l[1].split('|')
    # list_of_list for every attribute
    list_of_list = get_empty_list_of_list(attr_num)
    for record in record_list:
      attr_l = record.split(',')
      attr_list = [attr_l[i] for i in PROFILE_IDX]
      for i in xrange(0, len(attr_list)):
        list_of_list[i].append(attr_list[i])

    attr_res = []

    for idx in xrange(attr_num):
      # set
      if (idx == TIME_IND or idx == CITY_IND or idx == HID_IND):
        attr_res.append (set_to_string(set(list_of_list[idx])))
      # majority
      elif (idx < NUM_IND):
        attr_res.append (get_majority(list_of_list[idx]))
      # numerical
      else:
        attrs = list_of_list[idx]
        denom = len(attrs)
        num = 0
        for attr in attrs:
            num += int(attr)
        attr_res.append(str(float(num)/denom))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))
