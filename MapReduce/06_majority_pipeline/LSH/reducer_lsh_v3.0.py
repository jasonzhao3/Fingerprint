#!/usr/bin/env python
import sys
import math
from itertools import groupby
from operator import itemgetter


'''
   This map-reduce job is used to LSH profiles into multiple buckets.
   In contrast to the previous two versions, this version also includes the cluster procedure
   after LSH to avoid big impossible cluster (user).

   But the output format is still same as before, for the ease of future evaluation step

   Mapper output format:
        bucket_idx_group \t attribute_list

   Reducer output format:
        bucket_idx_group \t attribute_list
        
        !!! Note: if there're multiple users in one bucket, we simply append the user index to the bucket index
        e.g. bucket 8538_12m has three users => keys are 8538u1_12m, 8538u2_12m, 8538u3_12m

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
# threshold for grouping device into one user
THRESHOLD = 0.75

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

def cal_similarity(profile1, profile2):
     x_list = profile1.split(',')[:-1]
     y_list = profile2.split(',')[:-1]

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

def update_cluster_sim (device, cluster, cluster_sim):
  new_sim = 0
  for idx, ex_device in enumerate (cluster):
    tmp_sim = cal_similarity (device, ex_device)
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

def print_clusters (key, clusters):
    key_prefix, key_suffix = key.split('_')
    key_prefix += 'u'
    key_suffix = '_' + key_suffix
    for idx, user in enumerate(clusters):
        new_key = key_prefix + str(idx) + key_suffix
        for device in user:
            print '%s%s%s' % (new_key, "\t", device)


def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    for key, group in groupby(data, itemgetter(0)):
        # expand grouper into device_list
        device_list = []
        for key, device in group:
            device_list.append(device)

        # only do clustering if the bucket have more than 3 devices
        if (len(device_list) <= 3):
            for device in device_list:
                print '%s%s%s' % (key, "\t", device)
        
        else:
            try:
                clusters = []; clustroids = []; cluster_sims = []
                for device in device_list:
                    if (len(clustroids) == 0):
                        clusters.append ([device])
                        clustroids.append (device)
                        cluster_sims.append ([0])
                    else:
                        scores = [cal_similarity(device, clustroid) for clustroid in clustroids]
                        max_idx, max_sim = get_max_idx_value (scores)
                        if (max_sim > THRESHOLD):
                            add_device_to_cluster (device, max_idx, clusters, clustroids, cluster_sims)
                        else:
                            clusters.append ([device])
                            clustroids.append (device)
                            cluster_sims.append ([0])
                print_clusters (key, clusters)

            except (RuntimeError, TypeError, NameError, ValueError, IOError):
                # count was not a number, so silently discard this item
                print "ERROR!!"
                pass
 
if __name__ == "__main__":
    main()
