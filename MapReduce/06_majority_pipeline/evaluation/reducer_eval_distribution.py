#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter


'''
  This script is used to see the distribution of devices in each bucket

  Mapper: 
  ========
  input:
  bucket_idx_group \t device

  output:
  num_device_group \t correct or wrong
  correct_group \t similarity
  
  => Note: here we group num_of_device into following range:
  [1, 2, 3, 4, 5, 6, 7, 8+]

  Reducer:
  ========
  input:
  num_device_group \t correct or wrong bucket  
  correct/wrong_group \t sim

  output:
  num_device_group \t total_num and error_ratio
  correct/wrong_group \t sim_list
  
'''



def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one num_device group
    for key, group in groupby(data, itemgetter(0)):      
      
      try:
        
        if (key.split('_')[0] == 'correct' or 
            key.split('_')[0] == 'wrong'):
          sim_list = []
          for key, val in group:
            sim_list.append(val)
          print ("%s%s%s" % (key, '\t', ','.join(sim_list)))

        else:
          numBucket = 0
          numError = 0
          for key, val in group:
              numBucket += 1
              if (val == 'wrong'):
                numError += 1
          error_ratio = numError / numBucket
          val = str(numBucket) + '_' + str(error_ratio)
          print ("%s%s%s" % (key, separator, val))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

