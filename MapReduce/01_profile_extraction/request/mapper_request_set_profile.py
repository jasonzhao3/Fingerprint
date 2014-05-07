#!/usr/bin/env python
'''
    This map-reduce job takes in request record data,
    and output set-based profile of each device

    Mapper Output Format:
    identifier  \t  set-based feature list 
    e.g. 
    00WEb5zE9xX1ze \t 805,153,2037,false,Carlsbad,29|4,Verizon,32_223;31_392|32_94;31_392,76.176.121.33|70.211.14.65|70.211.9.239|70.211.2.23,3,8,1|0,none,1188236865|1286633761|1188235759|1188233751,1

    Its corresponding reducer is an identical reducer.
'''

import sys

PROFILE_IDX = [
               1, # publisher_id - majority
               2, # network_id - majority
               3, # domain_id - majority
               8, # is_not_yume_white_list  - ratio of true
               20, # city_name - jaccard set
               21, # census_DMA - majority
               31, # service_provider_name  - majority
               32, # key_value - jaccard set
               #35, # requested_date - frequency within 4 hours
               36, # ip_addr - jaccard set
               43, # service_provider - jaccard set
               45, # player_size - jaccard set
               47, # page_fold - majority
               49, # play_type - majority
               53, # hid - majority
               55, # is_on_premises - 1's ratio
               ];
attr_num = len(PROFILE_IDX);
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
        attr_list = [attr_l[i] for i in PROFILE_IDX]
        for i in range(0, len(attr_list)):
            attr_count[i].append(attr_list[i])
      string_res = '';
      for i in range(0, len(attr_count)):
        attrs = attr_count[i];
        if i == 4:
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
        else:
          m = set()
          for attr in attrs:
            m.add(attr)
          string_res += '|'.join(m)
        if i != len(attr_count)-1:
          string_res += ',';
      print '%s%s%s' % (l[0], "\t", string_res)

