#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter
from datetime import datetime
'''

   This map-reduce job is used to find large cluster with multiple devices of different types
   for later manual labeling

   Mapper output format:
        bucket_num \t device profile

   Reducer: identical reducer

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
        cluster = []
        dp_set = set()
        flag = False
        for key, device in group:
            cluster.append (device)
            dp = device.split(',')[-1].split('_')[1]
            dp_set.add(dp)
            
            if (len(dp_set) > 2):
              flag = True
            elif (len(dp_set) == 2 and '0' not in dp_set):
              flag = True

        if (flag and len(cluster) < 100):
          for device in cluster:
            print ("%s%s%s" %(key, '\t', device))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

