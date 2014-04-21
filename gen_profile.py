import sys, os, csv
from common.helper import *
from config.data_config import DATA_PATH as path
from config.data_config import MAJORITY_IDX, JACCARD_IDX, PROFILE_SCHEMA


data_file_name = "week-part0"
output_file_name = "device-profiles.csv"
data_file = os.path.join (path, data_file_name)
output_file = os.path.join (path, output_file_name)
NUM_FEATURES = 62
NUM_THREASHOD = 30

list_of_list = []
list_of_list.append (PROFILE_SCHEMA)
cnt = 0
with open (data_file) as f:
	for device in f:
		identifier, record_of_device = get_record_list (device)
		if (not is_valid_device (record_of_device, NUM_FEATURES, NUM_THREASHOD)):
			continue
		profile = []
		profile.append (identifier)
		# majority attributes
		for idx in MAJORITY_IDX:
			profile.append (get_majority (record_of_device, idx))
		# jaccard-set attributes
		for idx in JACCARD_IDX:
			profile.append (get_jaccard_set (record_of_device, idx))
		# is_yume_not_white_list, is_on_premises
		profile.append (get_ratio (record_of_device, 8, 'TRUE'))
		profile.append (get_ratio (record_of_device, 55, '1'))
		# request_data_stat
		profile.extend (get_freq_stat_list (record_of_device, 35))
		list_of_list.append (profile)
		print "add device", cnt
		cnt += 1

write_csv (output_file, list_of_list)


