from __future__ import division
import os
from os import listdir
from os.path import isfile, join



'''
	Combine result
'''
origin_path = '../../../../../local_data/evaluation_test/eval_test_version4.0'
origin_path = '../../../../../local_data/lsh_evaluation_iteration_v3'
# if isfile(join(path,f)) 
file_names = [ f for f in listdir(origin_path) if isfile(join(origin_path,f))]

ls = []
for file_name in file_names:
	file_name = join(origin_path, file_name)
	# print file_name
	with open (file_name) as f:
		for line in f:
			line = line.strip()
			if (line):
				ls.append(line)


# tuple_list format: (wrong_num, correct_num)
tuple_list = []
wrong_map = {}
correct_map = {}
for item in ls:
	bucket, val = item.split('\t')
	flag = bucket.split('_')[0]
	key = bucket.split('_')[1]
	if (flag == 'wrong'):
		wrong_map[key] = int(val)
	elif (flag == 'correct'):
		correct_map[key] = int(val)

wrong_keys = wrong_map.keys()
wrong_keys.sort()
correct_keys = correct_map.keys()
correct_keys.sort()
print wrong_keys
print correct_keys

val_list = []
key_list = []

total_keys = correct_map.keys()
total_keys.extend(wrong_map.keys())
total_keys = set(total_keys)

for key in total_keys:
	if key in wrong_map and key in correct_map:
		item = (wrong_map[key], correct_map[key])
	elif key in wrong_map:
		item = (wrong_map[key], 0)
	elif key in correct_map:
		item = (0, correct_map[key])

	key_list.append(key)
	val_list.append(item)

comb_list = zip(key_list, val_list)
comb_list.sort(key = lambda x:int(x[0][:-1]))

final_list = [item[1] for item in comb_list]
error_list = [item[0]/(item[0] + item[1]) for item in final_list]

print comb_list
print final_list
print error_list





