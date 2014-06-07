#!/usr/bin/env python
'''
    This map-reduce job takes in request and beacon device profile
    and output joined profile of each device

    Its corresponding mapper is an identical mapper

    Reducer output format:
    identifier \t joined feture list
'''

'''
  beacon profile length: 25
  request profile length: 27
'''


import sys
from itertools import groupby
from operator import itemgetter

TIME_IND = 3
CITY_IND = 5
HID_IND = 16
NUM_CATEGORICAL = 17

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def get_attr_list(device_list):
    list1 = device_list[0].split(',')
    list2 = device_list[1].split(',')
    if (len(list1) < len(list2)):
      return list2, list1
    else:
      return list1, list2

def merge_attr_list(attr_list1, attr_list2):
  tuple_list = attr_list1[-1].split('??')
  tuple_list.extend(attr_list2[-1].split('??'))
  tuple_list = list(set(tuple_list))
  tuple_list.sort(key=lambda t:t[0])
  
  attr_list = attr_list1[:-1]
  attr_list.extend(attr_list2[NUM_CATEGORICAL:-1])
  attr_list.append('??'.join(tuple_list))
  return attr_list


def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    for key, group in groupby(data, itemgetter(0)):
        try:
            device_list = []
            for key, device in group:
              device_list.append(device)
            # request and beacon need merge 
            if (len(device_list) == 2):
              request_list, beacon_list = get_attr_list(device_list)
              attr_list = merge_attr_list(request_list, beacon_list)
              emitted_value = ','.join(attr_list)
            else:
              emitted_value = device_list[0]
            print( "%s\t%s" % (key, emitted_value))

        except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
            print "ERROR!!"
            pass
 
if __name__ == "__main__":
    main()
