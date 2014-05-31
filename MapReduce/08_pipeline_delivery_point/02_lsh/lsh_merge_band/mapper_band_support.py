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
  Mapper: Cn2 => output (device1, device2) \t device1 profile || device2 profile
  Reducer: count support, remove lows support and low similarity pairs  => edge list

Another parallel step:
  output all identifiers (nodes)

'''

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def make_key(str1, str2):
  if (str1 < str2):
    return (str1, str2)
  else:
    return (str2, str1)

def emit_cluster(cluster):
  num_device = len(cluster)
  for i in xrange(num_device):
    key1 = cluster[i].split(',')[-1]
    for j in xrange(x+1, num_device):
      key2 = cluster[j].split(',')[-1]
      key = make_key (key1, key2)
      print ("%s%s%s" %(key, '\t', cluster[i] + '||' + cluster[j]))



# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        cluster = []
        for key, device in group:
            cluster.append (device)
  
        emit_cluster(cluster)

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

