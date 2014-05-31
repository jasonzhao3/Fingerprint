#!/usr/bin/env python

import sys

'''
   This map-reduce job is used to LSH profiles into multiple buckets

   Mapper output format:
        bucket_idx_group \t attribute_list

  Corresponding reducer: identical reducer

'''


import csv
from time import strptime, mktime
import numpy as np
import math

HID_IDX = 17

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    key = attr_list[17]
    print '%s%s%s' % (key, "\t", ','.join(attr_list))