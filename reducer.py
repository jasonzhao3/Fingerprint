#!/usr/bin/env python
from __future__ import division
import sys

def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if record1[i] != "null" or record2[i] != "null":
            denom = denom +1
            if record1[i] == record2[i]:
                num = num + 1   
    return num / denom

#def read_mapper_output(file, separator='\t'):
#    for line in file:
#        yield line.rstrip().split(separator, 1)
 
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
    
    #with open (filename) as f:
        #for input_line in f:
   for input_line in sys.stdin:
       input_line = input_line.strip()
       if not input_line:
           continue
       this_key, value = input_line.split("\t", 1)
       if last_key == this_key:
          running_features.append(value)
       else:
           if last_key:
               scores = []
               for x in range(len(running_features)):
                   for y in range(x+1, len(running_features)):
                       record1 = running_features[x].split(',')
                       record2 = running_features[y].split(',')
                       if len(record1) == len(record2):
                        scores.append(cal_jaccard(record1, record2))
               print ("%s%s%s" % (last_key, separator, ",".join(str(v) for v in scores)))
               running_features = []
           
           running_features.append(value)                
           last_key = this_key
 
   if last_key == this_key:
            scores = []
            for x in range(len(running_features)):
                for y in range(x+1, len(running_features)):
                    record1 = running_features[x].split(',')
                    record2 = running_features[y].split(',')
                    if len(record1) == len(record2):
                        scores.append(cal_jaccard(record1, record2))
            print ("%s%s%s" % (last_key, separator, ",".join(str(v) for v in scores)))
        
 
if __name__ == "__main__":
    main()
        

    