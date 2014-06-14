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

	cnt = 0
	for file_name in file_names:
		file_name = join(path, file_name)
		# print file_name
		with open (file_name) as f:
			for line in f:
				line = line.strip()
				cnt += int(line.split('\t')[1])
				
	print cnt



'''
	Combine result
'''
origin_path = '../../../../local_data/final_mix/outlier_device/'

# results is a list of edges
# 
results = combine_files(origin_path)


# with open('distribution_v5.1.json', 'w') as f:
# 	json.dump(distribution_json, f)

# with open('similarity_v5.1.json', 'w') as f:
# 	json.dump(sim_json, f)
