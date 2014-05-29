from __future__ import division
import os
from os import listdir
from os.path import isfile, join
import json
from collections import Counter

'''

This script is for a single bucket_group visualization,
e.g. 12m buckets => ouput distribution and similarity 


# output distribution json format: altogether 9 bins -- 8 types of users and one overal
# [{'user_number':[829103, 28392, 18202, 150322, 3820, ...]}, 
#  {'error_ratio': [0.0, 0.02, 0.023, 0.012]}]



# output similarity json format:
# [{'correct': 
					 [sim, type, ptg]
				{1: [[0.01, 1, 0.000], [0.02, 1, 0.000], [...]]},
				 2: [[0.01, 2, 0.000], [0.02, 2, 0.001], [...]]},
				 3: ...
				...
				 8: ...}
				
	},
	{'error': 
			    {1: [[0.01, 0.5, 0.000], [0.02, 0.5, 0.000], [...]]},
				 2: [[0.01, 1.5, 0.000], [0.02, 1.5, 0.001], [...]]},
				 3: ...}
				...
				 8: ...}
			  
	}
  ]


'''

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

def get_distribution_json (result_list):
	user_number_list = []
	error_ratio_list = []
	
	total_user_number = 0
	overal_error_number = 0

	for result in result_list:
		key, val = result.split('\t')
		user_type, group = key.split('_')
		val_list = val.split('_')
		user_number = int(val_list[0])
		error_ratio = float(val_list[1])

		total_user_number += user_number
		overal_error_number += user_number * error_ratio

		user_number_list.append((user_type, user_number))
		error_ratio_list.append((user_type, error_ratio))

	user_number_list.sort(key=lambda t:t[0])
	error_ratio_list.sort(key=lambda t:t[0])
	user_number_list = [item[1] for item in user_number_list]
	error_ratio_list = [item[1] for item in error_ratio_list]

	user_number_list.append(total_user_number)
	error_ratio_list.append(overal_error_number / total_user_number)

	json_obj = {}
	json_obj['user_number'] = user_number_list
	json_obj['error_ratio'] = error_ratio_list

	return json_obj

# use 100 bins
def cal_bin(value):
	return int(value / 0.01) * 0.01

def make_bubble(sim_str_list, y_axis):
	if (sim_str_list[0] == ''):
		return []
	sim_counter = Counter()
	for sim in sim_str_list:
		sim_counter[cal_bin(float(sim))] += 1

	sims = sim_counter.keys()
	counts = sim_counter.values()
	sum_count = sum(counts)
	areas = [count / sum_count * 100 for count in counts]
	bubbles = [[sims[i], y_axis, areas[i]] for i in xrange(len(sims))]
	return bubbles


def get_similarity_json (result_list):
	json_obj = {}
	json_obj['correct'] = {}
	json_obj['error'] = {}

	for result in result_list:
		key, val = result.split('\t')
	 	user_type, group = key.split('_')
		val_list = val.split('_')
		correct_sim_list = val_list[2].split(',')
		error_sim_list = val_list[3].split(',')

		correct_bubble_list = make_bubble(correct_sim_list, int(user_type))
		error_bubble_list = make_bubble(error_sim_list, int(user_type) + 0.5)

		json_obj['correct'][int(user_type)] = correct_bubble_list
		json_obj['error'][int(user_type)] = error_bubble_list

	return json_obj



'''
	Combine result
'''
origin_path = '../../../../local_data/pipeline_dp/version1.0/'

# results is a list of lsh evaluation result:
# e.g. 7_12m	28595_0.222731246721_0.8,0.9,0.93_0.3,0.4,0.3
results = combine_files(origin_path)

distribution_json = get_distribution_json(results)
sim_json = get_similarity_json(results)

with open('distribution_v1.0.json', 'w') as f:
	json.dump(distribution_json, f)

with open('similarity_v1.0.json', 'w') as f:
	json.dump(sim_json, f)
