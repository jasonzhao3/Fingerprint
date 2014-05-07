#!/usr/bin/env python

import sys, math, os

'''
    input: key is clustroid info: identifier_city, value is identifiers_cities
    
    output: key is clustroid_city, value is cities string, e.g.
    san jose \t san jose, san francisco, palo alto
'''
for line in sys.stdin:
      line = line.strip ()
      # get rid of empty line
      if (not line):
        continue
      key_city = line.split('\t')[0].split('_')[1]
      cities = line.split('\t')[1].split('_')[1]
      print '%s%s%s' % (key_city, "\t", cities)

# output: 0.8 10,18


