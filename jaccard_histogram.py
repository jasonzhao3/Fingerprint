import csv, os
from collections import Counter

def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)

def csv_reader (csv_file):
	data = []
	with open(csv_file, "rb") as f:
		reader = csv.reader(f)
		for row in reader:
			data.append (row)
	return data

def cal_jaccard (record_list):


path = '../data'
data_file_name = 'us_ca_sf_data.csv'
data_file = os.path.join (path, data_file_name)

data = csv_reader (data_file)

device_map = {}
ip_map = Counter ()


for record in data:
	cookie = record[22];
	attr_list = cookie.split('_')
	
	if (len(attr_list) == 5):
		ip_addr = attr_list[0] + '.' + attr_list[1] + '.' + attr_list[2] + '.' + attr_list[3]
		ip_map[ip_addr] += 1 
		if (attr_list[4] in device_map):
			device_map[attr_list[4]].append (record)
		else:
			record_list = []
			record_list.append (record)
			device_map[attr_list[4]] = record_list
	else:
		print attr_list

print len (data), len (device_map), len (ip_map)
print device_map.values()[:10]
# print max(device_map.values()), max(ip_map.values())


# write_csv ('../data/interest_records.csv', record_list)