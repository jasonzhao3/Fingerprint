#!/usr/bin/env python

import sys

'''
   This map-reduce job is used to analyze delivery point correlated attributes

   Take in device profile
Step1:
------
   Mapper output format:
        attribute_value \t delivery_point

   Reducer output format:
  		attribute  \t value_true/false

Step2:
------
	Mapper output format:
		attribute \t  supportNum_totNum

	Reducer: identical

'''
# length include the evaluation tuple
BEACON_LEN = 25

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    delivery_point = l[0].split('_')[1]

    attr_length = len(attr_list)
    if (attr_length == BEACON_LEN):
      for idx in xrange(attr_length-8):
        print '%s%s%s' % (str(idx)+','+ attr_list[idx], "\t", delivery_point)
      for idx in xrange(attr_length-8, attr_length-1):
        print '%s%s%s' % (str(idx+9)+','+ attr_list[idx], "\t", delivery_point)
    else:
      for idx in xrange(len(attr_list)-1):
        print '%s%s%s' % (str(idx)+','+ attr_list[idx], "\t", delivery_point)
