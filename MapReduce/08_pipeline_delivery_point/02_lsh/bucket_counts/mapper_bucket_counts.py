#!/usr/bin/env python
import sys

for input_line in sys.stdin:
     line = input_line.strip ()
     key, profile = line.split('\t')
     print '%s%s%d' % (key, "\t", 1) 

