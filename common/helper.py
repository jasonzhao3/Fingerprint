import csv
from time import strptime, mktime
import numpy as np

'''
 	CSV read/write helpers
'''

# Write a list of list into a csv file
def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)


# Read a csv file into a list of list
def read_csv (csv_file):
	data = []
	with open(csv_file, "rb") as f:
		reader = csv.reader(f)
		for row in reader:
			data.append (row)
	return data


'''
	parse cookie field
'''

def get_identifier_and_ip (cookie):
	attr_list = cookie.split ('_')
	if (len(attr_list) == 5):
		ip_addr = attr_list[0] + '.' + attr_list[1] + '.' + attr_list[2] + '.' + attr_list[3]
		return attr_list[4], ip_addr
	elif (len(attr_list) == 1):
		return "null", attr_list[0]
	else:
		return "null", "null"


'''
	Jaccard similarity calculation helpers
'''

# assuming length of r1 and r2 are same
def cal_jaccard (r1, r2):
	num = 0
	denom = 0    
	for i in xrange(len(r1)):
	    if ((r1[i] != "null" or r2[i] != "null")
	       and (r1[i] != "0" or r2[i] != "0")):
	    	denom = denom +1
	    	if r1[i] == r2[i]:
	    		num = num + 1
	return num / denom

# given a list of records, generate a list of pair-pair jaccard similarity
def get_jaccard_dist_list (records):
	dist_list = []
	for i in xrange (len(records)):
		for j in xrange (i+1, len(records)):
			dist_list.append (cal_jaccard (records[i], records[j]))
	return dist_list

'''
	Profile helpers
'''
def most_common(lst):
    return max(set(lst), key=lst.count)

# record_list is a list of list
def get_record_list (record_str):
	record_str = record_str.strip ()
	identifier = record_str.split('\t')[0]
	record_list = record_str.split ('\t')[1].split('|')
	list_of_list = []
	for record in record_list:
		list_of_list.append (record.split(','))
	return identifier, list_of_list

def get_majority (list_of_list, attr_idx):
	attr_list = [curr_list[attr_idx] for curr_list in list_of_list]
	return most_common (attr_list)

def get_jaccard_set (list_of_list, attr_idx):
	attr_list = [curr_list[attr_idx] for curr_list in list_of_list]
	return set (attr_list)

def get_ratio (list_of_list, attr_idx, flag):
	attr_list = [curr_list[attr_idx] for curr_list in list_of_list]
	flag_count = 0	
	for attr in attr_list:
		if (attr == flag):
			flag_count += 1
	return float(flag_count) / len (attr_list)

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
def cal_interval (str1, str2):
	str1 = str1.split('.')[0]
	str2 = str2.split('.')[0]
	# print str1, str2
	t1 = mktime (strptime (str1, TIME_FORMAT))
	t2 = mktime (strptime (str2, TIME_FORMAT))
	return t2 - t1

# return list of avg frequency, variability
def get_freq_stat_list (list_of_list, attr_idx):
	attr_list = [curr_list[attr_idx] for curr_list in list_of_list]
	attr_list.sort ()
	interval_list = []
	for i in xrange (0, len (attr_list)-1):
		interval_list.append (cal_interval (attr_list[i], attr_list[i+1]))
	mean = np.mean (interval_list)
	std = np.std (interval_list)
	return [mean, std]

def is_valid_device (list_of_list, num_features, num_threshold):
	if (len (list_of_list) > num_threshold):
		for ls in list_of_list:
			# print len(ls)
			if (len (ls) != num_features):
				return False
		return True
	else:
		return False




