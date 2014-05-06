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
   clustroids = []
   clusters = []
   for input_line in sys.stdin:
       input_line = input_line.strip()
       if not input_line:
           continue
       this_key, value = input_line.split("\t", 1)
       if last_key == this_key and this_key:
          running_features.append(value)
       else:
           if last_key:
               count = 0
               for x in running_features:
                   x_list = x.split(',')
                   request1 = [x_list[i] for i in range(15) if i != CITY_IND]
                   beacon1 = x_list[15:-1]
                   if count == 0:
                       new_cluster = []
                       new_cluster.append(x)
                       clusters.append(new_cluster)
                       clustroids.append(x)
                   else:
                       scores = []
                       for y in clustroids:
                           y_list = y.split(',')
                           request2 = [y_list[i] for i in range(15) if i != CITY_IND]
                           beacon2 = y_list[15:-1]
                           if len(request1) == len(request2) and len(beacon1) == len(beacon2):
                               scores.append(0.7 * cal_jaccard(request1, request2) + 0.3 * cal_cosine(beacon1, beacon2))
                       maxVal = 0.0
                       maxInd = -1
                       for i in range(len(scores)):
                           if(scores(i) > maxVal):
                               maxVal = scores(i)
                               maxInd = i
                       if maxVal >= 0.8:
                           clusters[maxInd].append(x)
                           clustroids[maxInd] = getClustroid(clusters[maxInd]) #update clustroids
                       else:
                           new_cluster = []
                           new_cluster.append(x)
                           clusters.append(new_cluster)
                           clustroids.append(x)
                   count += 1
               
               #printing output
               for cluster in clusters:
                   value_list = []
                   city_list = []
                   for x in cluster:
                       x_list = x.split(',')
                       value_list.append(x_list[-1])
                       city_list.append(x_list[CITY_IND])
                   print ("%s%s%s" % (str(0.8), separator, ','.join(value_list) + '_' + ','.join(city_list)))    
                       
               running_features = []
               clusters = []
               clustroids = []
           
           running_features.append(value)                
           last_key = this_key       
       
   if last_key == this_key and this_key:
       count = 0
       for x in running_features:
           x_list = x.split(',')
           request1 = [x_list[i] for i in range(15) if i != CITY_IND]
           beacon1 = x_list[15:-1]
           if count == 0:
               new_cluster = []
               new_cluster.append(x)
               clusters.append(new_cluster)
               clustroids.append(x)
           else:
               scores = []
               for y in clustroids:
                   y_list = y.split(',')
                   request2 = [y_list[i] for i in range(15) if i != CITY_IND]
                   beacon2 = y_list[15:-1]
                   if len(request1) == len(request2) and len(beacon1) == len(beacon2):
                       scores.append(0.7 * cal_jaccard(request1, request2) + 0.3 * cal_cosine(beacon1, beacon2))
               maxVal = 0.0
               maxInd = -1
               for i in range(len(scores)):
                   if(scores(i) > maxVal):
                       maxVal = scores(i)
                       maxInd = i
               if maxVal >= 0.8:
                   clusters[maxInd].append(x)
                   clustroids[maxInd] = getClustroid(clusters[maxInd]) #update clustroids
               else:
                   new_cluster = []
                   new_cluster.append(x)
                   clusters.append(new_cluster)
                   clustroids.append(x)
           count += 1
       
       #printing output
       for cluster in clusters:
           value_list = []
           city_list = []
           for x in cluster:
               x_list = x.split(',')
               value_list.append(x_list[-1])
               city_list.append(x_list[CITY_IND])
           print ("%s%s%s" % (str(0.8), separator, ','.join(value_list) + '_' + ','.join(city_list)))   
        
 
if __name__ == "__main__":
    main()
        
        
