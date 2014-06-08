#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
import math

'''
   This map-reduce job assigns different weights for attributes
   
   This map-reduce job is used to emit edges from LSH bucket

   Mapper output format:
        (start_node, end_node) \t profile1 || profile2

   Reducer output format:
        (sart_node, end_node) \t similarity_location_eval (possible dp eval)


'''


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

# only allow one bucket mistake
def pass_support_check(support_cnt):
  return support_cnt >= 1


# concat version
def main(separator='\t'):
    
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        support_cnt = 0
        correct_flag = True
        for pair, profiles in group:
          support_cnt += 1
          if (profiles.split('_')[1] == 'False'):
            correct_flag = False
        
        val_str = profiles.split('_')[0] + '_' + str(correct_flag)
        if (pass_support_check(support_cnt)):
            print ("%s%s%s" %(key, '\t', val_str))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "Reducer ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

