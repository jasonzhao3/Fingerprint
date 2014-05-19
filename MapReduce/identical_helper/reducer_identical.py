#!/usr/bin/env python
from __future__ import division
import sys
import math

def main(separator='\t'):
	for input_line in sys.stdin:
		line = input_line.strip ()
		print line
 
if __name__ == "__main__":
    main()