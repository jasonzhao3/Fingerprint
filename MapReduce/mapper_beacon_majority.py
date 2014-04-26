#!/usr/bin/env python

import sys


BEACON_FILTER_IDX = [
    0, # session_id
    1, # domain_id
    2, # placement_id
    3, # advertisement_id
    4, # conversionPixelld
    5, # zero_tracker
    6, # twentry_five
    7, # fifty
    8, # seventry_five
    9, # one_hundred
    10, # click_tracker
    11, # customization_id
    12, # custom_type_id
    13, # custom_report_id
    14, # custom_event_id
    15, # is_media_buy
    16, # request_time
    #22, # viewer_id
    27, # volume_percent
    30, # state
    31, # city
    48, # player_location
    49  # player_size
]
               
attr_num = len(BEACON_FILTER_IDX);
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into words
    record_list = l[1].split('|')
    if len(record_list) >= 30:
      attr_count = []
      for i in range(0, attr_num):
          attr_count.append([])
      for record in record_list:
          attr_l = record.split(',')
          attr_list = [attr_l[i] for i in BEACON_FILTER_IDX]
          for i in range(0, len(attr_list)):
            attr_count[i].append(attr_list[i])
      attr_res = []
      for attrs in attr_count:
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
        attr_res.append(maxItem)
      print '%s%s%s' % (l[0], "\t", ','.join(attr_res))

