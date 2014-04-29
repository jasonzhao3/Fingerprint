import sys, os
# from config.data_config import LOCAL_DATA_PATH as path
from collections import Counter
import pylab as plt
from common.helper import write_csv
from common.similarity_helper import *
import numpy as np


'''
	test pair pair similarity
'''

# local test
path = '../local_data/join_request_beacon/'
data_file_names = ["part-00000", "part-00001", "part-00002"]
output_file_name = "part012_sim_approx_result_0.8"
output_file = os.path.join (path, output_file_name)


profile_list = []
device_list = []
# read three files into memory
for data_file_name in data_file_names:
	data_file = os.path.join (path, data_file_name)


	with open (data_file) as f:
		for line in f:
			line = line.strip ()
			profile_list.append (line.split ('\t')[1].split(','))
			device_list.append (line.split('\t')[0])

	print len (profile_list)

# construct pair-pair counter
sim_counter = Counter()
for i in xrange (len(profile_list)):
	for j in xrange(i+1, len(profile_list)):
		key = tuple_to_key (device_list[i], device_list[j])
		# sim_counter[key] = cal_exact_match_ratio (profile_list[i], profile_list[j])
		sim_counter[key] = cal_approx_match_ratio (profile_list[i], profile_list[j])

with open (output_file, "wb") as f:
	for key, val in sim_counter.iteritems():
		if (val > 0.8):
			print key, val
			f.write(key[0] + ',' + key[1] + '\t' + str(val))
			f.write('\n')




