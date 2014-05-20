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
    running_total = []
         
    for input_line in sys.stdin:
       input_line = input_line.strip()
       if input_line != "":
           this_key, value = input_line.split("\t")
         
           if last_key == this_key:
               running_total.append(value)
           else:
              if last_key:
                  if (len(running_total) == 2):
                      list1 = running_total[0].split(',')
                      list2 = running_total[1].split(',')
                      if (len(list1) < len(list2)):
                        list1 = running_total[1].split(',')
                        list2 = running_total[0].split(',')
                      idx = [3, 5, 17]
                      for i in idx:
                        setR = set(list1[i].split('|')) 
                        setB = set(list2[i].split('|')) 
                        setU = setR | setB
                        list1[i] = '|'.join(setU)
                      list1.extend(list2[20:])  
                      emitted_value = ','.join(list1)
                      print( "%s\t%s" % (last_key, emitted_value))
                  if (len(running_total) == 1):
                      print( "%s\t%s" % (last_key, running_total[0]))

              running_total = []    
              running_total.append(value)
              last_key = this_key
         
    if last_key == this_key and last_key:
      if (len(running_total) == 2):
          list1 = running_total[0].split(',')
          list2 = running_total[1].split(',')
          if (len(list1) < len(list2)):
            list1 = running_total[1].split(',')
            list2 = running_total[0].split(',')
          idx = [3, 5, 17]
          for i in idx:
            setR = set(list1[i].split('|')) 
            setB = set(list2[i].split('|')) 
            setU = setR | setB
            list1[i] = '|'.join(setU)
          list1.extend(list2[20:])  
          emitted_value = ','.join(list1)
          print( "%s\t%s" % (last_key, emitted_value))
      if (len(running_total) == 1):
          print( "%s\t%s" % (last_key, running_total[0]))

if __name__ == "__main__":
    main()
