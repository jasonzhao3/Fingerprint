from __future__ import division
import os
from os import listdir
from os.path import isfile, join
import json
from collections import Counter


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

# key: num_device  val: total_num
# key: num_device  val: error_ratio
def build_histogram_map (result_list):
	num_map = {}
	error_ratio_map = {}
	for result in result_list:
		key, val = result.split('\t')
		num_device, group = key.split('_')
		if (num_device == 'correct' or num_device == 'wrong'):
			continue
		else:
			num_device = int(num_device)
			tot_num, error_ratio = val.split('_')
			if (num_device in num_map):
				num_map[num_device].append((group, tot_num))
				error_ratio_map[num_device].append((group, error_ratio))
			else:
				num_map[num_device] = [(group, tot_num)]
				error_ratio_map[num_device] = [(group, error_ratio)]

	for key in num_map.keys():
		num_list = num_map[key]
		num_list.sort(key = lambda x:int(x[0][:-1]))
		num_map[key] = [int(item[1]) for item in num_list]

		ratio_list = error_ratio_map[key]
		ratio_list.sort(key = lambda x:int(x[0][:-1]))
		error_ratio_map[key] = [float(item[1]) for item in ratio_list]

	return [num_map, error_ratio_map]

# use 100 bins
def cal_bin(value):
	return int(value / 0.01) * 0.01

def scatter_histogram(stat_map):
	for key, val_list in stat_map.iteritems():
		sim_counter = Counter()
		for val in val_list:
			sim_counter[cal_bin(val)] += 1
		
		sims = sim_counter.keys()
		counts = sim_counter.values()
		points = [[sims[i], counts[i]] for i in xrange(len(sims))]
		stat_map[key] = points


def get_similarity_histogram (result_list):
	correct_map = {}
	error_map = {}
	for result in result_list:
		key, val = result.split('\t')
		flag, group = key.split('_')
		if (flag != 'correct' and flag != 'wrong'):
			continue
		else:
			if (flag == 'correct'):
				val_list = val.split(',')
				correct_map[group] = [float(item) for item in val_list]
			else:
				val_list = val.split(',')
				error_map[group] = [float(item) for item in val_list]
	
	scatter_histogram (correct_map)
	scatter_histogram (error_map)
	
	return [correct_map, error_map]



def get_overal_stat (json_list):
	num_user_list = []
	error_user_list = []
	# i corresponds to each num_bucket_group
	for i in xrange(len(json_list[0][1])):
		num_user = 0
		error_user = 0
		# j corresponds to each num_device_histogram_bin
		for j in xrange(1, 9):
			num_user += json_list[0][j][i]
			error_user += int(json_list[0][j][i] * json_list[1][j][i])
		
		num_user_list.append(num_user)
		error_user_list.append(error_user)

	error_ratio = [error_user_list[i] / num_user_list[i] for i in xrange(len(num_user_list))]
	overal_stat = {}
	overal_stat['total_user_num'] = num_user_list
	overal_stat['total_error_ratio'] = error_ratio
	json_list.append (overal_stat)
	return json_list



'''
	Combine result
'''
origin_path = '../../../../local_data/evaluation_distribution/version3.1'


results = combine_files(origin_path)


# device-user distribution
json_list = build_histogram_map(results)
result_json = get_overal_stat(json_list)

with open('distribution_v3.1.json', 'w') as f:
	json.dump(result_json, f)

# similarity-user distribution
result_json = get_similarity_histogram(results)
with open('sim_distribution_v3.1.json', 'w') as f:
	json.dump(result_json, f)


