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

BEACON_LEN = 26

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    delivery_point = l[0].split('_')[1]

    attr_length = len(attr_list)
    if (attr_length == 26):
    	for idx in xrange(attr_length-7):
    		if idx != 3 and idx != 5:
    			print '%s%s%s' % (str(idx)+','+ attr_list[idx], "\t", delivery_point)
    	for idx in xrange(attr_length-6, attr_length):
    		print '%s%s%s' % (str(idx+10)+','+ attr_list[idx], "\t", delivery_point)
    else:
    	for idx in xrange(len(attr_list)):
    		if idx != 3 and idx != 5:
    			print '%s%s%s' % (str(idx)+','+ attr_list[idx], "\t", delivery_point)
