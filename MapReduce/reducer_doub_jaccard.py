#!/usr/bin/env python
from __future__ import division
import sys

def double_jaccard (record1, record2):
    scores = []
    for i in range(len(record1)):
        num = 0
        denom = 0  
        set1 = set(record1[i].split(','))
        set2 = set(record2[i].split(','))
        intersect = set1.intersection(set2)
        for elem in set1.union(set2):
            if elem.lower() != "null" and elem.lower() != "n/a":
                denom = denom + 1            
                if elem in intersect:
                    num = num + 1
        if (denom > 0):
            scores.append(num / denom)
    return sum(scores)/len(scores)
 
def main(separator='\t'):
    #filename = 'test.txt'
    #data = read_mapper_output(filename, '\t')
    #data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
   last_key = None
   records = {} 
    #with open (filename) as f:
        #for input_line in f:
   for input_line in sys.stdin:
       input_line = input_line.strip()
       if not input_line:
           continue
       this_key, value = input_line.split("\t", 1)
       #print(this_key)
       records[this_key] = value
       if last_key:
           for prev in records:
               if (prev != this_key):
                   profile1 = records[prev].split('|')
                   profile2 = records[this_key].split('|')
                   if len(profile1) == len(profile2):
                       emitted_key = prev + "," + this_key
                       score = double_jaccard(profile1, profile2)
                       if (score >= 0.5):
                           print ("%s%s%s" % (emitted_key, separator, str(score)))
       last_key = this_key
 
if __name__ == "__main__":
    main()
        

    
