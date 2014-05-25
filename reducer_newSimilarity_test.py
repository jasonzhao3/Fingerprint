#!/usr/bin/env python
import sys
import math
from itertools import groupby
from operator import itemgetter

'''
  reducer input:  
      bucket_idx \t profile1
      bucket_idx \t profile2
      ...
  reducer output: each line is a cluster 
      bucket_idx  \t identifier1, identifier2, ..._city1, city2, ...

  Then by using bucket_idx, we can compare three groups of buckets
  This can also be used to evaluate how good our LSH is...
'''

CITY_IND = 5
TIME_IND = 3
HID_IND = 17
REQ_ONLY = 31
BEC_ONLY = 27
RB_COMMON = 20
BEC_CAT = 21
RB_UNION_CAT = 32
RB_UNION = 38
IS_PREMISE = 18
IS_PREFETCH = 28
THRESHOLD = 0.893

def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if (record1[i].lower() != "null" or record2[i].lower() != "null") and (record1[i].lower() != "n/a" or record2[i].lower() != "n/a") and (record1[i].lower() != "na" or record2[i].lower() != "na"): 
            if i != IS_PREFETCH and i != IS_PREMISE:
                if record1[i] != '0' and record2[i] != '0': 
                    denom = denom + 1
                    if record1[i] == record2[i]:
                        num = num + 1 
            else:
                denom = denom + 1
                if record1[i] == record2[i]:
                    num = num + 1   
    return float(num) / denom
 
def cal_cosine(record1, record2):
    cross = 0.0
    norm1 = 0.0
    norm2 = 0.0
    for i in range(len(record1)):
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

def getSimilarity(profile1, profile2):
     x_list = profile1.split(',')

     y_list = profile2.split(',')
     score = 0.0
     if len(x_list) > len(y_list):
         #swap x_list and y_list, so len(x) <= len(y)
         tmp = x_list
         x_list = y_list
         y_list = tmp
     if len(x_list) == len(y_list):
         if len(x_list) == REQ_ONLY:
             request1 = [x_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             score = cal_jaccard(request1, request2)
         elif len(x_list) == BEC_ONLY:
             request1 = [x_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[BEC_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         else:
             request1 = [x_list[i] for i in range(RB_UNION_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(RB_UNION_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             beacon1 = x_list[RB_UNION_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.8 * cal_jaccard(request1, request2) + 0.2 * cal_cosine(beacon1, beacon2)
     else:
         if len(x_list) == BEC_ONLY and len(y_list) == REQ_ONLY:
             request1 = [x_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             score = cal_jaccard(request1, request2)
         elif len(x_list) == BEC_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in range(BEC_CAT) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(RB_COMMON) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2.append(y_list[REQ_ONLY])
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         elif len(x_list) == REQ_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             request2 = [y_list[i] for i in range(REQ_ONLY) if i != CITY_IND and i!= TIME_IND and i != HID_IND]
             score = cal_jaccard(request1, request2)
     return score
    
def main(separator='\t'):
    #data = read_mapper_output(filename, '\t')
    #data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
   last_key = None
   this_key = None
   running_features = []
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
                       profile1 = ','.join(x_list[:-1])
                       x_id = x_list[-1]
                       y_list = running_features[y].split(',')
                       profile2 = ','.join(y_list[:-1])
                       y_id = y_list[-1]
                       score = getSimilarity(profile1, profile2)
                       emit_key = x_id + ',' + y_id
                       print ("%s%s%s" % (emit_key, separator, str(score)))
               running_features = []
           
           running_features.append(value)                
           last_key = this_key       
       
   if last_key == this_key and this_key:
           for x in range(len(running_features)):
                   for y in range(x+1, len(running_features)):
                       x_list = running_features[x].split(',')
                       profile1 = ','.join(x_list[:-1])
                       x_id = x_list[-1]
                       y_list = running_features[y].split(',')
                       profile2 = ','.join(y_list[:-1])
                       y_id = y_list[-1]
                       score = getSimilarity(profile1, profile2)
                       emit_key = x_id + ',' + y_id
                       print ("%s%s%s" % (emit_key, separator, str(score)))
        
 
if __name__ == "__main__":
    main()
        
        
