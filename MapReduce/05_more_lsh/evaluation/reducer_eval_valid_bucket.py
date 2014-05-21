#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter


'''
  output:
  bucket_idx  \t  num of devices in this bucket
  numDevice \t  total number of device
  numBucket \t total number of buckets
'''
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 

        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):      
      try:
        numBucket = 0
        for key, val in group:
          if (key == 'numBucket'):
            numBucket += int(val);
          else:
            print ("%s%s%s" % (key, separator, val))
        
        if (key == 'numBucket'):
          print ("%s%s%d" % (key, separator, numBucket))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 
if __name__ == "__main__":
    main()

