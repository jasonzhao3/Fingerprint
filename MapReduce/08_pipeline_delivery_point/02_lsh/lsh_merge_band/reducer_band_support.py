#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
'''

Step 1:
   This map-reduce job is used to LSH profiles into multiple buckets and bands

   Mapper output format:
        bucket_idx_band \t profile, identifier

  Corresponding reducer: identical reducer

Step 2:
  Mapper: Cn2 => output (device1, device2) \t 1
  Reducer: count support, remove lows support and low similarity pairs  => edge list

Another parallel step:
  output all identifiers (nodes)

TODO: the two steps can be concated eventually, and the intermediate output is on HDFS rather than s3

'''

FULL_LENGTH = 36  # 7 bands
REQUEST_LENGTH = 29 # 5 bands
BEACON_LENGTH = 26 # 5 bands

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def make_key(str1, str2):
  if (str1 < str2):
    return (str1, str2)
  else:
    return (str2, str1)

def pass_support_check(device1, device2, support_cnt):
  if (len(device1) == )



# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        support_cnt = 0
        device1 = "null"; device2 = "null";
        for pair, profiles in group:
          if (support_cnt == 0):
            device1, device2 = profiles.split('||')
          support_cnt += 1
  
        if (pass_support_check(device1, device2, support_cnt)):
          device1 = device1.split(',')
          device2 = device2.split(',')
          sim = cal_sim(device1[:-1], device2[:-1])
          if (sim > 0.6):
            key1 = device1[-1]
            key2 = device2[-1]
            key = make_key(key1, key2)
            print ("%s%s%.6f" %(key, '\t', sim))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

