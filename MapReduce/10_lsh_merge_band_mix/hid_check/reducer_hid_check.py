#!/usr/bin/env python
'''
    This map-reduce job takes in request and beacon device profile
    and output joined profile of each device

    Its corresponding mapper is an identical mapper

    Reducer output format:
    identifier \t joined feture list
'''

import sys

def main(separator='\t'):
    last_key = None
    this_key = None
    running_total = set()
         
    for input_line in sys.stdin:
       input_line = input_line.strip()
       if input_line != "":
           this_key, value = input_line.split("\t")
         
           if last_key == this_key:
               running_total.add(value)
           else:
              if last_key:
                count = len(running_total.pop().split('|'))  
                print( "%s\t%s" % (last_key, count))
              running_total = set()   
              running_total.add(value)
              last_key = this_key
         
    if last_key == this_key and last_key:
                count = len(running_total.pop().split('|'))  
                print( "%s\t%s" % (last_key, count))

if __name__ == "__main__":
    main()