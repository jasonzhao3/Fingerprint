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
				line = re.sub('[()]','', line)
				if (line):
					ls.append(line)
	return ls



'''
	Combine result
'''
origin_path = '../../../../local_data/graph_data/cluster/version1.0/'

# results is a list of edges
# 
results = combine_files(origin_path)
print results


# with open('distribution_v5.1.json', 'w') as f:
# 	json.dump(distribution_json, f)

# with open('similarity_v5.1.json', 'w') as f:
# 	json.dump(sim_json, f)
