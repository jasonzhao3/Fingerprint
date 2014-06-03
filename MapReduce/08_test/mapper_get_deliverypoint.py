#!/usr/bin/env python
'''

'''

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
      line = line.strip()
      identifier, value = line.split('\t')
      fields = value.split(',')
      delivery_point = fields[8].split('|')
      delivery_point = set(delivery_point)
      delivery_point = delivery_point - set(['0'])
      if len(delivery_point) > 1:
      	print '%s%s%s' % ('case: ', "\t", line)
