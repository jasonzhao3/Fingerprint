#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys
 
'''
   This map-reduce job is used to analyze delivery point correlated attributes

Step1:
------
   Mapper output format:
        attribute_value \t delivery_point

   Reducer output format:
        attribute  \t value_true/false

Step2:
------
    Mapper output format:
        attribute \t  supportNum_totNum

    Reducer: identical

'''

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

 
def is_full_support(dp_set):
    if (len(dp_set) == 5):
        return True
    elif (len(dp_set) == 4 and '0' not in dp_set):
        return True
    else:
        return False

def is_two_support(dp_set):
    if (len(dp_set) == 3):
      return True
    elif (len(dp_set) == 2 and '0' not in dp_set):
      return True
    else:
      return False


def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)

    for current_attribute, group in groupby(data, itemgetter(0)):
        try:
            tot_cnt = 0; true_cnt = 0
            for key, val in group:
                flag = val.split(',')[1]
                tot_cnt += 1
                if (flag == 'true'):
                  true_cnt += 1

            print '%s%s%s' % (key, '\t', str(tot_cnt) + '_' + str(true_cnt))

        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()

