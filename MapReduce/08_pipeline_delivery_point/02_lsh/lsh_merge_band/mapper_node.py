#!/usr/bin/env python

import sys
from random import randint
'''

Parallel step:
  output all identifiers (nodes)

  reducer output: node_num  \t concat_node with ','
'''

seed = randint(0,10)
# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    identifier = l[0]
    print '%d%s%s' % (seed, "\t", identifier)
    

