#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 

 
def main(separator='\t'):
    print "start to output\n"
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      num_device = 0
      # if numDevice: key="numDevice", val = num of device
      # else: key = bucket_idx, val = device profile
      try:
        for key, val in group:
          if (key == 'numDevice'):
            num_device += int(val)
          else:
            print ("%s%s%s" % (key, separator, val))
        
        if (key == 'numDevice'):
          print ("%s%s%d" % (key, separator, num_device))

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
 
if __name__ == "__main__":
    main()
        
# concat version
# def main(separator='\t'):
#     print "start to output\n"
#   # input comes from STDIN (standard input)
#     data = read_mapper_output(sys.stdin, separator=separator)
   
#     # one group corresponds to one bucket
#     for key, group in groupby(data, itemgetter(0)):   
#       print key    
#       num_device = 0
#       device_list = []
#       # if numDevice: key="numDevice", val = num of device
#       # else: key = bucket_idx, val = device profile
#       try:
#         for key, val in group:
#           if (key == 'numDevice'):
#            num_device += int(val)
#           else:
#             device_list.append(val)
        
#         if (key != 'numDevice'):
#           print ("%s%s%s" % (key, separator, '||'.join(device_list)))
#         else:
#           print ("%s%s%d" % (key, separator, num_device))

#       except (RuntimeError, TypeError, NameError, ValueError, IOError):
#             # count was not a number, so silently discard this item
#         print "ERROR!!"
#         pass
 
# if __name__ == "__main__":
#     main()
        
     