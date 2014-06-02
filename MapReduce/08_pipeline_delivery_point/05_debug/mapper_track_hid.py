#!/usr/bin/env python

import sys

'''
   This map-reduce job is used to LSH profiles into multiple buckets

   Mapper output format:
        bucket_idx_group \t attribute_list

  Corresponding reducer: identical reducer

'''


HID_IDX = 16

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    key = attr_list[HID_IDX]
    print '%s%s%s' % (key, "\t", ','.join(attr_list))