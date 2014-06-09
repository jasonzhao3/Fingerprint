#!/usr/bin/env python
import sys, csv, math, os
import random
 # sys.path.append(os.path.dirname(__file__))

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    if '\t' in line:
        key, value = line.split('\t')
        attr_list = value.split(',')
        perm = range(len(attr_list))
        random.shuffle(perm)
        str_list = [str(perm[i]) for i in range(len(perm))]
    print '%s%s%s' % (key, "\t", ','.join(str_list))
   
