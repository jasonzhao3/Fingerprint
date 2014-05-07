import os
from os import listdir
from os.path import isfile, join

'''
	Combine election result
'''
origin_path = '../../../../local_data/output_election_eval/'
# if isfile(join(path,f)) 
path_names = [ f for f in listdir(origin_path)]

# print path_names

for path_name in path_names:
	file_names = [f for f in listdir(join(origin_path, path_name))]
	path_prefix = join(origin_path, path_name)
	error_cnt = 0; user_cnt = 0; device_cnt = 0
	for file_name in file_names:
		file_name = join(path_prefix, file_name)
		# print file_name
		with open (file_name) as f:
			line = f.readline()
			line = line.strip()
			# print line
			try:
				num_list = line.split('\t')
				if (len(num_list) == 2):
					num_list = num_list[1].split(',')
					error_cnt += int (num_list[0])
					user_cnt += int (num_list[1])
					device_cnt += int (num_list[2])
			except (RuntimeError, TypeError, NameError, ValueError, IOError):
				pass
	print path_name, error_cnt, user_cnt, device_cnt




# '''
# 	Combine clustroid result
# '''
# origin_path = '../../../../local_data/output_clustroid_eval/'
# # if isfile(join(path,f)) 
# path_names = [ f for f in listdir(origin_path)]

# # print path_names

# for path_name in path_names:
# 	file_names = [f for f in listdir(join(origin_path, path_name))]
# 	path_prefix = join(origin_path, path_name)
# 	error_cnt = 0; total_cnt = 0; user_cnt = 0; device_cnt = 0
# 	for file_name in file_names:
# 		file_name = join(path_prefix, file_name)
# 		# print file_name
# 		with open (file_name) as f:
# 			line = f.readline()
# 			line = line.strip()
# 			# print line
# 			try:
# 				num_list = line.split('\t')
# 				if (len(num_list) == 2):
# 					num_list = num_list[1].split(',')
# 					error_cnt += int (num_list[0])
# 					total_cnt += int (num_list[1])
# 					user_cnt += int (num_list[2])
# 					device_cnt += int (num_list[3])
# 			except (RuntimeError, TypeError, NameError, ValueError, IOError):
# 				pass
# 	print path_name, error_cnt, total_cnt, user_cnt, device_cnt



