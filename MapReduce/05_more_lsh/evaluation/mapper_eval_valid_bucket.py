#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter


'''
  This script is used to count how many buckets are not empty (i.e. valid)

  output:
  bucket_idx  \t  num of devices in this bucket
  numDevice \t  total number of device
  numBucket \t total number of buckets within this reducer
'''
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 

        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    num_bucket = 0
    for key, group in groupby(data, itemgetter(0)):   
      num_bucket += 1 
      num_device = 0
      num_device_within_bucket = 0
      # if numDevice: key="numDevice", val = num of device
      # else: key = bucket_idx, val = device profile
      try:
        for key, val in group:
          if (key == 'numDevice'):
           print ("%s%s%d" % (key, separator, num_device))
           num_bucket -= 1
          else:
            num_device_within_bucket += 1
        
        if (key != 'numDevice'):
          print ("%s%s%d" % (key, separator, num_device_within_bucket))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
    print ("%s%s%d" % ('numBucket', separator, num_bucket))
 
if __name__ == "__main__":
    main()
        