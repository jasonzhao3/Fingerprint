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

def update_cluster_sim (device, cluster, cluster_sim):
  new_sim = 0
  for idx, ex_device in enumerate (cluster):
    tmp_sim = getSimilarity (device, ex_device)
    cluster_sim[idx] += tmp_sim
    new_sim += tmp_sim
  cluster_sim.append (new_sim)

def add_device_to_cluster (device, max_idx, clusters, clustroids, cluster_sims):
  cluster = clusters[max_idx]
  cluster_sim = cluster_sims[max_idx]
  # first update cluster_sim
  update_cluster_sim (device, cluster, cluster_sim)
  # then update clusters
  cluster.append (device)
  # finally update clustroids
  clustroid_idx, sim_val = get_max_idx_value (cluster_sim)
  clustroids[max_idx] = cluster[clustroid_idx]


def main(separator='\t'):
 	# input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
   	# one group corresponds to one bucket
   	# TODO: different group of buckets share same devices
    for key, group in groupby(data, itemgetter(0)):
        try:
            #cluster_sims: keep a record of accumulated similarity of each device within a cluster
            clusters = []; clustroids = []; cluster_sims = []
            for key, device in group:
                # print key, device
            	if (len(clustroids) == 0):
            		clusters.append ([device])
            		clustroids.append (device)
                        cluster_sims.append ([0])
            	else:
            		scores = [getSimilarity(device, clustroid) for clustroid in clustroids]
            		max_idx, max_sim = get_max_idx_value (scores)
            		if (max_sim > THRESHOLD):
                            # update clusters, clustroids, cluster_sims
                            add_device_to_cluster (device, max_idx, clusters, clustroids, cluster_sims)
            		else:
            			clusters.append ([device])
            			clustroids.append (device)
                                cluster_sims.append ([0])
            # print for each group (i.e. bucket)
            print_clusters (key, clusters)

        except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
            print "ERROR!!"
            pass
 
if __name__ == "__main__":
    main()
        
        
