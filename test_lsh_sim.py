import sys, os
# from config.data_config import LOCAL_DATA_PATH as path
from collections import Counter
import pylab as plt
from common.helper import *
from common.similarity_helper import *
import numpy as np


def add_pp_sim (sim_counter, identifier_list, device_map):
	for i in xrange (len(identifier_list)):
		for j in xrange (i+1, len(identifier_list)):
			profile1 = device_map[identifier_list[i]]
			profile2 = device_map[identifier_list[j]]
			key = tuple_to_key (identifier_list[i], identifier_list[j])
			# sim_counter[key] = cal_exact_match_ratio (profile1, profile2)
			sim_counter[key] = cal_approx_match_ratio (profile1, profile2)

path = '../local_data/join_request_beacon/'
data_file_names = ["part-00000", "part-00001", "part-00002"]
output_file_name = "part012_sim_approx_lsh_result_0.8"
output_file = os.path.join (path, output_file_name)


device_map = {}
# read three files into memory
for data_file_name in data_file_names:
	data_file = os.path.join (path, data_file_name)

	with open (data_file) as f:
		for line in f:
			line = line.strip ()
			device_map[line.split('\t')[0]] = line.split('\t')[1].split(',')
	print len (device_map)


# lsh process
bucket_group1 = create_empty_buckets ()
bucket_group2 = create_empty_buckets ()
bucket_group3 = create_empty_buckets ()

for identifier, profile in device_map.iteritems():
	bucket_idxs = get_rb_hash_buckets (profile)
	# print bucket_idxs
	bucket_group1[bucket_idxs[0]].append (identifier)
	bucket_group2[bucket_idxs[1]].append (identifier)
	bucket_group3[bucket_idxs[2]].append (identifier)

# print to tune hash parameters
'''
cnt = 0
for i in xrange(len(bucket_group1)):
	if (len(bucket_group3[i]) > 0):
		cnt += 1
		print bucket_group3[i]
	# print len(bucket_group3[i])
print cnt
'''
# construct pair-pair counter in each bucket -- now just repeat the code
sim_counter = Counter ()
for i in xrange (len(bucket_group1)):
	if len(bucket_group1[i]) >= 2:
		add_pp_sim (sim_counter, bucket_group1[i], device_map)

for i in xrange (len(bucket_group2)):
	if len(bucket_group2[i]) >= 2:
		add_pp_sim (sim_counter, bucket_group2[i], device_map)

for i in xrange (len(bucket_group3)):
	if len(bucket_group3[i]) >= 2:
		add_pp_sim (sim_counter, bucket_group3[i], device_map)

with open (output_file, "wb") as f:
	for key, val in sim_counter.iteritems():
		if (val > 0.8):
			print key, val
			f.write(key[0] + ',' + key[1] + '\t' + str(val))
			f.write('\n')




