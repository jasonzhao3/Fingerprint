from __future__ import division
import os
from os import listdir
from os.path import isfile, join
import json
from collections import Counter
import re

def combine_files(path):
	# if isfile(join(path,f)) 
	file_names = [ f for f in listdir(path) if isfile(join(path,f))]

	ls = []
	for file_name in file_names:
		file_name = join(path, file_name)
		# print file_name
		with open (file_name) as f:
			for line in f:
				line = line.strip()
				if (line):
					ls.append(line)
	return ls



'''
	Combine result
'''
origin_path = '../../../../local_data/final_majority/dp_distribution/'

# results is a list of edges
# 
results = combine_files(origin_path)

tot_counter = Counter()
true_counter = Counter()
for result in results:
	attr, val = result.split('\t')
	tot_cnt = int(val.split('_')[0])
	true_cnt = int(val.split('_')[1])
	tot_counter[attr] += tot_cnt
	true_counter[attr] += true_cnt

print tot_counter
print len(tot_counter)
print "\n"
print true_counter
print len(true_counter)


ratio_counter = {}
for key in tot_counter.keys():
	ratio_counter[key] = true_counter[key] / tot_counter[key]

print ratio_counter

keys = ratio_counter.keys()
vals = ratio_counter.values()
comb_list = zip(keys, vals)
comb_list.sort(key=lambda t:t[1], reverse=True)

print comb_list

# with open('distribution_v5.1.json', 'w') as f:
# 	json.dump(distribution_json, f)

# with open('similarity_v5.1.json', 'w') as f:
# 	json.dump(sim_json, f)
