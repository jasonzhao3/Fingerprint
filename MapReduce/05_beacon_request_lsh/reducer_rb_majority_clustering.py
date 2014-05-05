#!/usr/bin/env python
from __future__ import division
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

CITY_IND = 4
THRESHOLD = 0.9


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

 
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def get_max_idx_value (my_list):
	index, value = max(enumerate(my_list), key=itemgetter(1))
	return index, value


def print_clusters (key, clusters):
	for cluster in clusters:
		identifier_list = []; city_list = []
		for device in cluster:
			attr_list = device.split (',')
			identifier_list.append (attr_list[-1])
			city_list.append (attr_list[CITY_IND])
        # print key, city_list
        print ("%s%s%s" % (key, '\t', ','.join(identifier_list) + '_' + ','.join(city_list)))    


def main(separator='\t'):
 	# input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
   	# one group corresponds to one bucket
   	# TODO: different group of buckets share same devices
    for key, group in groupby(data, itemgetter(0)):
        try:
            clusters = []; clustroids = []
            for key, device in group:
                # print key, device
            	if (len(clustroids) == 0):
            		clusters.append ([device])
            		clustroids.append (device)
            	else:
            		scores = [getSimilarity(device, clustroid) for clustroid in clustroids]
            		max_idx, max_sim = get_max_idx_value (scores)
            		if (max_sim > THRESHOLD):
            			clusters[max_idx].append (device)
            			clustroids[max_idx] = getClustroid(clusters[max_idx]) 
            		else:
            			clusters.append ([device])
            			clustroids.append (device)
            # print for each group (i.e. bucket)
            print_clusters (key, clusters)

        except (RuntimeError, TypeError, NameError, ValueError):
            # count was not a number, so silently discard this item
            print "ERROR!!"
            pass

 
if __name__ == "__main__":
    main()
        
        
