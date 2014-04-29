#!/usr/bin/env python
from __future__ import division
import sys
import math

# where is the grouping process?????
# can we make sure that the subsequent a few records are with the same key ???

def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if (record1[i].lower() != "null" or record2[i].lower() != "null") and (record1[i].lower() != "n/a" or record2[i].lower() != "n/a") and (record1[i].lower() != "na" or record2[i].lower() != "na"): 
            denom = denom +1
            if record1[i] == record2[i]:
                num = num + 1   
    return num / denom
 
def cal_cosine(record1, record2):
    cross = 0.0
    norm1 = 0.0
    norm2 = 0.0
    for i in range(len(record1)):
        if (record1[i].lower() != "null" and record2[i].lower() != "null") and (record1[i].lower() != "n/a" and record2[i].lower() != "n/a") and (record1[i].lower() != "na" and record2[i].lower() != "na"):
            r1 = float(record1[i])
            r2 = float(record2[i])            
            cross += r1*r2
            norm1 += r1*r1
            norm2 += r2*r2
            
    denom = math.sqrt(norm1) * math.sqrt(norm2)
    if denom != 0:
        #print cross / denom
        return cross / denom
    else:
        return 0.0
    
def main(separator='\t'):
    #data = read_mapper_output(filename, '\t')
    #data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
   CITY_IND = 4
   last_key = None
   this_key = None
   running_features = []
    #with open (filename) as f:
        #for input_line in f:
   for input_line in sys.stdin:
       input_line = input_line.strip()
       if not input_line:
           continue
       this_key, value = input_line.split("\t", 1)
       if last_key == this_key and this_key:
          running_features.append(value)
       else:
           if last_key:
               for x in range(len(running_features)):
                   for y in range(x+1, len(running_features)):
                       x_list = running_features[x].split(',')
                       x_id = x_list[-1]
                       x_city = x_list[CITY_IND]
                       y_list = running_features[y].split(',')
                       y_id = y_list[-1]
                       y_city = y_list[CITY_IND]
                       request1 = [x_list[i] for i in range(15) if i != CITY_IND]
                       request2 = [y_list[i] for i in range(15) if i != CITY_IND]
                       beacon1 = x_list[15:-1]
                       beacon2 = y_list[15:-1]
                       if len(request1) == len(request2) and len(beacon1) == len(beacon2):
                           score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
                           emit_key = x_id + ',' + y_id
                           if (score >= 0.5):
                              print ("%s%s%s" % (emit_key, separator, str(score) + '_' + x_city + '_'+ y_city))
               running_features = []
           
           running_features.append(value)                
           last_key = this_key       
       
   if last_key == this_key and this_key:
           for x in range(len(running_features)):
                   for y in range(x+1, len(running_features)):
                       x_list = running_features[x].split(',')
                       x_id = x_list[-1]
                       x_city = x_list[CITY_IND]
                       y_list = running_features[y].split(',')
                       y_id = y_list[-1]
                       y_city = y_list[CITY_IND]
                       request1 = [x_list[i] for i in range(15) if i != CITY_IND]
                       request2 = [y_list[i] for i in range(15) if i != CITY_IND]
                       beacon1 = x_list[15:-1]
                       beacon2 = y_list[15:-1]
                       if len(request1) == len(request2) and len(beacon1) == len(beacon2):
                           score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
                           emit_key = x_id + ',' + y_id
                           if (score >= 0.5):
                              print ("%s%s%s" % (emit_key, separator, str(score) + '_' + x_city + '_'+ y_city))
        
 
if __name__ == "__main__":
    main()
        
        
