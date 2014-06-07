#!/usr/bin/env python
from __future__ import division
import sys
from itertools import groupby
from operator import itemgetter
from datetime import datetime

'''

   This map-reduce job is used to emit edges from LSH bucket

   Mapper output format:
        (start_node, end_node) \t profile1 || profile2

   Reducer output format:
        (sart_node, end_node) \t similarity_location_eval (possible dp eval)


'''


CLUSTER_LIMIT_HINT = 10
THRESHOLD = 0.83
HARD_LIMIT = 10000





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




# request_skip_set and beacon_skip_set are the subset of this
skip_list = \
  [   
    1, # placement_id (skip 0)
    2, # advertisement_id (skip 0)
    5, # content_video_id  (skip 0)
    7, # key_value (skip 0_0)
    12, #ovp_version (skip NA)
    13, #ovp_type (skip NA)
    14, #hid (skip 0)
    16, #audience_segment (skip NULL)
    17, # referrer_site (skip NULL)
    22, # publisher_channel_id (skip 0)
    23, # content_profile_id (skipp null)
  ]

skip_value_dict = \
{ 
  1: '0',
  2: '0',
  5: '0',
  7: '0_0',
  12: 'na',
  13: 'na',
  14: '0',
  16: 'null',
  17: 'null',
  22: '0',
  23: 'null'
}

skip_expectation_dict = \
  {
    1: 1.0/3670, # placement_id
    2: 1.0/1533, #advertsiment_id
    5: 1.0/23, # content_video_id  (skip 0)
    7: 1.0/962, # key_value (skip 0_0)
    12: 1.0/11, #ovp_version (skip NA)
    13: 1.0/11, #ovp_type (skip NA)
    14: 1.0/1452216, #hid (skip 0)
    16: 1.0/16, #audience_segment (skip NULL)
    17: 1.0/17, # referrer_site (skip NULL)
    22: 1.0/344, # publisher_channel_id (skip 0)
    23: 1.0/23, # content_profile_id (skipp null1/231/962 
  }


def cal_jaccard (record1, record2):
    num = 0
    denom = len(record1)    
    comp_list1 = [record1[i] for i in xrange(denom) if i not in skip_list]
    comp_list2 = [record2[i] for i in xrange(denom) if i not in skip_list]
    num = sum([1 for i in xrange(len(comp_list1)) if comp_list1[i] == comp_list2[i]])

    # deal with skip list
    for i in skip_list:
      if (i < denom):
        if (record1[i] == skip_value_dict[i] or record2[i] == skip_value_dict[i]):
          num += skip_expectation_dict[i]
        elif (record1[i] == record2[i]):
          num += 1
    
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

# exclude identifier and evaluation field
BEC_ONLY = 24
REQ_ONLY = 26
RB_UNION = 33

BEC_CAT = 18
RB_UNION_CAT = 27
RB_COMMON = 17


# is that ok to use expecation value to calculate similarity for all other undefined value ???
def cal_similarity(profile1, profile2):
     # last attribute is identifier, and the second by last is the evaluation metric
     x_list = profile1.split(',')[:-2]
     y_list = profile2.split(',')[:-2]

     score = 0.0
     if len(x_list) > len(y_list):
         #swap x_list and y_list, so len(x) <= len(y)
         tmp = x_list
         x_list = y_list
         y_list = tmp
     if len(x_list) == len(y_list):
         if len(x_list) == REQ_ONLY:
             score = cal_jaccard(x_list, y_list)
         elif len(x_list) == BEC_ONLY:
             request1 = [x_list[i] for i in xrange(BEC_CAT)]
             request2 = [y_list[i] for i in xrange(BEC_CAT)]
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[BEC_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         else:
             request1 = [x_list[i] for i in xrange(RB_UNION_CAT)]
             request2 = [y_list[i] for i in xrange(RB_UNION_CAT)]
             beacon1 = x_list[RB_UNION_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.8 * cal_jaccard(request1, request2) + 0.2 * cal_cosine(beacon1, beacon2)
     else:
         if len(x_list) == BEC_ONLY and len(y_list) == REQ_ONLY:
             request1 = [x_list[i] for i in xrange(RB_COMMON)]
             request2 = [y_list[i] for i in xrange(RB_COMMON)]
             score = cal_jaccard(request1, request2)
         elif len(x_list) == BEC_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in xrange(BEC_CAT)]
             request2 = [y_list[i] for i in xrange(RB_COMMON)]
             request2.append(y_list[REQ_ONLY])
             beacon1 = x_list[BEC_CAT:]
             beacon2 = y_list[RB_UNION_CAT:]
             score = 0.75 * cal_jaccard(request1, request2) + 0.25 * cal_cosine(beacon1, beacon2)
         elif len(x_list) == REQ_ONLY and len(y_list) == RB_UNION:
             request1 = [x_list[i] for i in range(REQ_ONLY)]
             request2 = [y_list[i] for i in range(REQ_ONLY)]
             score = cal_jaccard(request1, request2)
     return score


def further_cluster(device_list):
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
  return clusters

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def make_key(str1, str2):
  if (str1 < str2):
    return (str1, str2)
  else:
    return (str2, str1)


def emit_cluster(cluster):
  num_device = len(cluster)
  for i in xrange(num_device):
    key1 = cluster[i].split(',')[-1]
    for j in xrange(i+1, num_device):
      key2 = cluster[j].split(',')[-1]
      key = make_key (key1, key2)
      print ("%s%s%s" %(key, '\t', cluster[i] + '||' + cluster[j]))

def emit_clusters(clusters):
  for cluster in clusters:
    emit_cluster(cluster)



# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        cluster = []
        for key, device in group:
            cluster.append (device)
        
        if (len(cluster) > HARD_LIMIT):
          continue

        if (len(cluster) > CLUSTER_LIMIT_HINT):
            clusters = further_cluster(cluster)
            emit_clusters(clusters)
        else:
            emit_cluster(cluster)

      except (RuntimeError, TypeError, NameError, ValueError, IOError):
        print "Mapper ERROR!!"
        pass
 

if __name__ == "__main__":
    main()

