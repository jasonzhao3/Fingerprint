#!/usr/bin/env python
from __future__ import division
import sys
import math

'''
  reducer input:  
      bucket_idx \t profile1
      bucket_idx \t profile2
      ...
  reducer output: each line is a cluster 
      threshold  \t identifier1, identifier2, ..._city1, city2, ...

'''


# where is the grouping process?????
# can we make sure that the subsequent a few records are with the same key ???
CITY_IND = 4

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

def getSimilarity(profile1, profile2):
     x_list = profile1.split(',')
     y_list = profile2.split(',')
     request1 = [x_list[i] for i in range(15) if i != CITY_IND]
     request2 = [y_list[i] for i in range(15) if i != CITY_IND]
     beacon1 = x_list[15:-1]
     beacon2 = y_list[15:-1]
     score = 0.0
     if len(request1) == len(request2) and len(beacon1) == len(beacon2):
         score = 0.7 * cal_jaccard(request1, request2) + 0.3 * cal_cosine(beacon1, beacon2)
     return score

def getClustroid(cluster):
	maxSim = 0
	centroid = cluster[0]
	for i in range(0,len(cluster)):
		simTot = 0
		for j in range(0,len(cluster)):
			if j != i:
				simTot += getSimilarity(cluster[i],cluster[j])
		if simTot > maxSim:
			maxSim = simTot
			centroid = cluster[i]
	return centroid   
 
def main(separator='\t'):

   last_key = None
   this_key = None
   running_features = []
   #clustroids = []
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
                   if count == 0:
                       new_cluster = []
                       new_cluster.append(x)
                       clusters.append(new_cluster)
                   else:
                       scores = []
                       for cluster in clusters:
                           print len(cluster)
                           y = getClustroid(cluster) 
                           scores.append(getSimilarity(x, y))
                       maxVal = 0.0
                       maxInd = -1
                       for i in range(len(scores)):
                           if(scores[i] > maxVal):
                               maxVal = scores[i]
                               maxInd = i
                       #print maxVal
                       if maxVal >= 0.8:                          
                           clusters[maxInd].append(x)
                       else:
                           new_cluster = []
                           new_cluster.append(x)
                           clusters.append(new_cluster)
                   #print (len(clusters))
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
           
           running_features.append(value)                
           last_key = this_key       
       
   if last_key == this_key and this_key:
               count = 0
               for x in running_features:
                   if count == 0:
                       new_cluster = []
                       new_cluster.append(x)
                       clusters.append(new_cluster)
                   else:
                       scores = []
                       for cluster in clusters:
                           y = getClustroid(cluster) 
                           scores.append(getSimilarity(x, y))
                       maxVal = 0.0
                       maxInd = -1
                       for i in range(len(scores)):
                           if(scores[i] > maxVal):
                               maxVal = scores[i]
                               maxInd = i
                       #print maxVal
                       if maxVal >= 0.8:
                          # print maxInd                           
                           clusters[maxInd].append(x)
                       else:
                           new_cluster = []
                           new_cluster.append(x)
                           clusters.append(new_cluster)
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
        
 
if __name__ == "__main__":
    main()
        
        
