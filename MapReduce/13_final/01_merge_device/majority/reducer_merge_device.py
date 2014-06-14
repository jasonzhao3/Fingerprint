#!/usr/bin/env python
from __future__ import division
import sys
import hashlib
from random import randint
from itertools import groupby
from operator import itemgetter

'''

Step 1:
   This map-reduce job is used to combine multiple identifiers that belong to the same online/TV device
   For mobile and tablet device, the script just skips them

  input: joined profile
  output: new joined profile after merging 

   Mapper output format:
        hash_bucket \t profile
        e.g. for online/TV, multiple profiles may belong to the same hash_bucket
             for mobile/tablet, unique identifier as hash_bucket
   Reducer output format:
        new_identifier \t profile
        e.g. for online/TV, new_identifier is the concated identifier
        e.g. for mobile/tablet, new_identifier is the origial identifier

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
        delivery_point = key.split('_')[-1]
        
        # skip mobile/tablet
        if (delivery_point == '2' or delivery_point == '3'):
          for key, device in group:
            print '%s%s%s' % (key, '\t', device)
        else:
          identifier_list = []
          for key, device in group:
              identifier_list.append (device.split(',')[-1].split('_')[0])
              device_profile = device
          # construct new profile
          new_identifier = '|'.join(identifier_list) + '_1'
          device_profile = device.split(',')
          device_profile_str = ','.join(device_profile[:-1])
          print '%s%s%s' % (new_identifier, '\t', device_profile_str)
      except (RuntimeError, TypeError, NameError, ValueError, IOError):
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

