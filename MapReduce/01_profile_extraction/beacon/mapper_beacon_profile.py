#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

PROFILE_IDX = [0, # zero_tracker
               1, # twentry_five
               2, # fifty
               3, # seventry_five
               4, # one_hundred
               ];
attr_num = len(PROFILE_IDX)
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into words
    record_list = l[1].split('|')
    if len(record_list) >= 30:
      attr_count = [0] * (attr_num+1)
      for record in record_list:
        attr_list = record.split(',')
        hasOne = False
        for i in range(0, len(attr_list)):
          if attr_list[i].strip() == '1': 
            attr_count[i] += 1
            hasOne = True
        if not hasOne:
          attr_count[attr_num] += 1
      for i in range(0, len(attr_count)):
        attr_count[i] = float(attr_count[i])/len(record_list) 
        attr_count[i] = str(attr_count[i])
      print '%s%s%s' % (l[0], "\t", ','.join(attr_count))