#!/usr/bin/env python

import sys

'''

Parallel step:
  output all identifiers (nodes)

'''


# input comes from STDIN (standard input)
cnt = 0
node_list = []
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    node = l[1]
    node_list.append(node)
    cnt += 1
print ("%d%s%s" %(cnt, '\t', ','.join(node_list)))