#!/usr/bin/env python

import sys, math, os

'''
    input: key is bucket idx, value is identifiers_cities
    output: key is bucket group (i.e. 1, 2, 3), value is cities string, e.g.
    1 \t san jose, san francisco, palo alto
'''
for line in sys.stdin:
      line = line.strip ()
      # get rid of empty line
      if (not line):
        continue
      bucket_group = line.split('\t')[0][-1]
      # device_list = line.split('\t')[1].split('_')[0].split(',')
      cities = line.split('\t')[1].split('_')[1]
      # print bucket_group
      print '%s%s%s' % (bucket_group, "\t", cities)

# output: 0.8 10,18


