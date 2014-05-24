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
  user_type__group \t correct or wrong_similarity
  
  => Note: here we group num_of_device into following range:
  [1, 2, 3, 4, 5, 6, 7, 8+]

  Reducer:
  ========
  input:
  user_type__group \t correct or wrong_similarity
  

  output:
  user_type__group \t user_num_____error_ratio_______correct_similarity____error_similarity
  
'''




def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # user_type: has one/two/three devices
    # users: the group of users that belong to this user_type
    for user_type, users in groupby(data, itemgetter(0)):      
      
      try:

        num_user = 0
        num_error = 0
        correct_sim_list = []
        error_sim_list = []

        for key, user_info in users:
            num_user += 1
            flag, sim = user_info.split('_')
            if (flag == 'error'):
              num_error += 1
              error_sim_list.append(sim)
            else:
              correct_sim_list.append(sim)

        error_ratio = num_error / num_user
        user_stat = str(num_user) + '_' + str(error_ratio) + '_'
        correct_sim_str = ','.join(correct_sim_list)
        correct_sim_str += '_'
        error_sim_str = ','.join(error_sim_list)

        print ("%s%s%s%s%s" % (user_type, separator, user_stat, correct_sim_str, error_sim_str))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 

if __name__ == "__main__":
    main()
