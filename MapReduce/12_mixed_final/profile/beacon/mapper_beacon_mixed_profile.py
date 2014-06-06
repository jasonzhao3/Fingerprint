#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier_deliverypoint  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

# Note: these IND is one less than the PROFILE_IDX
CITY_IND = 5
TIME_IND = 3
HID_IND = 16
NUM_IND = 20  # number of majority features
NUM_RECORD_LIMIT = 800

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
               19  # slate_id
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
    '0', # slate id
    ############################     
    None, # zero_tracker
    None, # twentry_five
    None, # fifty
    None, # seventry_five
    None, # one_hundred
    None # volume percent
]


# skip some nonsense value when bulding majority profile
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

attr_num = len(PROFILE_IDX)
# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    record_list = l[1].split('|')

    if (len(record_list) > NUM_RECORD_LIMIT):
      continue

    # list_of_list for every attribute
    list_of_list = get_empty_list_of_list(attr_num)
    for record in record_list:
      attr_l = record.split(',')
      attr_list = [attr_l[i] for i in PROFILE_IDX]
      for i in xrange(0, len(attr_list)):
        list_of_list[i].append(attr_list[i])

    comb_time_location(list_of_list)
    attr_res = []

    for idx in xrange(len(list_of_list)):
      # separate time and city are skipped
      if (idx == TIME_IND or idx == CITY_IND):
        continue
      # majority
      elif (idx < NUM_IND):
        attr_res.append (get_majority(list_of_list[idx], skip_list[idx]))
      elif (idx in set_idx):
        attr_res.append (set_to_string(set(list_of_list[idx])))
      elif (idx == len(list_of_list) - 1):
        attr_res.append (tuple_list_to_str(list_of_list[idx]))
      else:
        attrs = list_of_list[idx]
        denom = len(attrs)
        num = 0
        for attr in attrs:
            num += int(attr)
        attr_res.append(str(float(num)/denom))

    print '%s%s%s' % (l[0], "\t", ','.join(attr_res))
