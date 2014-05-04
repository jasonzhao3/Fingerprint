import csv
from time import strptime, mktime
import numpy as np
import math
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
		interval = cal_interval (attr_list[i], attr_list[i+1])
		# 90 min
		if (interval < 60 * 90):
			interval_list.append (interval)
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

'''
	LSH hash function
'''
NUM_BUCKET = 500
# strong hash to 1000 buckets
def hash_bucket_1 (publisher_id, network_id, domain_id):
	# publisher_id = int (publisher_id)
	# network_id = int (network_id)
	# domain_id = int (domain_id)
	return ((17 * publisher_id) + (13 * network_id) + (3 * domain_id)) % NUM_BUCKET

def hash_bucket_2 (dma, hid, service_provider):
	return ((17 * dma) + (3 * hid) + service_provider) % NUM_BUCKET

# hash service_provide_name -- based on dan bernstein in comp.lang.c
def hash_string (input_str):
    djb2_code = 5381
    for i in xrange (0, len (input_str)):
        char = input_str[i];
        djb2_code = (djb2_code << 5) + djb2_code + ord (char)
    return djb2_code % NUM_BUCKET

# profile is an attribute list
def get_hash_buckets (profile):
	if (profile[0].isdigit ()):
		publisher_id = int (profile[0])
	else:
		publisher_id = 3333 #heuristic value for NA
	
	if (profile[1].isdigit ()):
		network_id = int (profile[1])
	else:
		network_id = 3333 

	if (profile[2].isdigit ()):
		domain_id = int (profile[2])
	else:
		domain_id = 3333
	
	if (profile[3].isdigit ()):
		dma = int (profile[3])
	else:
		dma = 3333

	if (profile[7].isdigit ()):
		hid = int (profile[7])
	else:
		hid = 3333
		
	service_provider = hash_string (profile[4])

	bucket1 = hash_bucket_1 (publisher_id, network_id, domain_id)
	bucket2 = hash_bucket_2 (dma, hid, service_provider)
	return bucket1, bucket2

def clear_nan (profile):
	if (profile[0].isdigit ()):
		publisher_id = int (profile[0])
	else:
		publisher_id = 3333 #heuristic value for NA
	
	if (profile[1].isdigit ()):
		network_id = int (profile[1])
	else:
		network_id = 3333 

	if (profile[2].isdigit ()):
		domain_id = int (profile[2])
	else:
		domain_id = 3333
	
	if (profile[3].isdigit ()):
		dma = int (profile[3])
	else:
		dma = 3333

	if (profile[7].isdigit ()):
		hid = int (profile[7])
	else:
		hid = 3333

	return publisher_id, network_id, domain_id, dma, hid


def hash_beacon_ptg (profile):
	ptg_0 = float (profile[-6])
	ptg_25 = float (profile[-5])
	ptg_50 = float (profile[-4])
	ptg_75 = float (profile[-3])
	ptg_100 = float (profile[-2])
	ptg_NA = float (profile[-1])

	return int (1024 * ptg_100 + 512 * ptg_75 + \
		    256 * ptg_50 + 128 * ptg_25 + 64 * ptg_0 + 32 * ptg_NA) % NUM_BUCKET


# get request_beacon hash buckets
def get_rb_hash_buckets (profile):
	publisher_id, network_id, domain_id, dma, hid = clear_nan (profile)
	bucket1 = (publisher_id + network_id + domain_id) % NUM_BUCKET
	# profile[6]: service_provider_name
	bucket2 = (dma + hid + hash_string(profile[6])) % NUM_BUCKET
	bucket3 = hash_beacon_ptg (profile)

	return [bucket1, bucket2, bucket3]


def create_empty_buckets ():
	buckets = {}
	for i in xrange(NUM_BUCKET):
		buckets[i] = []
	return buckets

