#!/usr/bin/env python

import sys
'''

Parallel step:
  output all identifiers (nodes)

  reducer output: node_num  \t concat_node with ','
'''

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    l = line.split('\t')
    key = l[0].strip('('')')
    sim, flag = l[1].split('_')
    emitted = key+ '_' + str(flag)
    if float(sim) >= 0.6:
        print '%d%s%s' % (1, "\t", emitted)
