#!/usr/bin/env python
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
                           running_total[0] = ','.join(list2)
                           running_total[1] = ','.join(list1)
                           
                       emitted_value = ','.join(running_total)
                       print( "%s\t%s" % (last_key, emitted_value))
               running_total = []    
               running_total.append(value)
               last_key = this_key
         
    if last_key == this_key and last_key:
        if (len(running_total) == 2):
            list1 = running_total[0].split(',')
            list2 = running_total[1].split(',')
            if (len(list1) < len(list2)):
               running_total[0] = ','.join(list2)
               running_total[1] = ','.join(list1)
            emitted_value = ','.join(running_total)
            print( "%s\t%s" % (last_key, emitted_value))

if __name__ == "__main__":
    main()
