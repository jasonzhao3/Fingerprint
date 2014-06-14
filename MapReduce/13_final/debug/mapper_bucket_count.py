#!/usr/bin/env python
import sys

for input_line in sys.stdin:
	line = input_line.strip ()
	band, val = line.split('\t')
	bucket = band.split('_')[-1]
	print '%s%s%d' % (bucket, "\t", 1) 
 
