#!/usr/bin/env python
import sys

 
def main(separator='\t'):
    #filename = 'test.txt'
    #data = read_mapper_output(filename, '\t')
    #data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
   last_key = None
   running_features = []
    
   for input_line in sys.stdin:
       input_line = input_line.strip()
       if not input_line:
           continue
       this_key, value = input_line.split("\t", 1)
       if last_key == this_key:
          running_features.append(value)
       else:
           if last_key:
               print ("%s%s%s" % (last_key, separator, "|".join(v for v in running_features)))
               running_features = []
           
           running_features.append(value)                
           last_key = this_key
 
   if last_key == this_key:
      print ("%s%s%s" % (last_key, separator, "|".join(v for v in running_features)))
        
 
if __name__ == "__main__":
    main()
        

    

