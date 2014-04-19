from __future__ import division
import csv, os
from collections import Counter
import pylab as plt


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

def get_jaccard_dist_list (records):
	dist_list = []
	for i in xrange (len(records)):
		for j in xrange (i+1, len(records)):
			dist_list.append (cal_jaccard (records[i], records[j]))
	return dist_list

def get_dist_hist (dist_list, bucket_num):
	dist_hist_list = [0] * bucket_num
	step = 1.0 / bucket_num
	for dist in dist_list:
		for i in xrange (bucket_num):
			if (dist > i * step and dist < (i+1) * step):
				dist_hist_list[i] += 1
	return dist_hist_list

def is_same_device (r1, r2):
	cookie1 = r1[22];
	cookie2 = r2[22];
	return cookie1.split('_')[4] == cookie2.split('_')[4]


path = '../data'
data_file_name = 'us_ca_sf_data.csv'
data_file = os.path.join (path, data_file_name)
data = csv_reader (data_file)

# device_map: key=identifier; val = list of records
# ip_map: key=ip, val = # of records

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
		data.remove (record)

print len (data), len (device_map), len (ip_map)
# print device_map.values()[:10]
# print max(device_map.values()), max(ip_map.values())

'''
 get jaccard distance within records of one single device
'''
jaccard_dist_list = []
for device in device_map.keys():
	records = device_map[device];
	jaccard_dist_list.extend (get_jaccard_dist_list (records))

# print jaccard_dist_list
print len (jaccard_dist_list)


'''
	Get cross-device jaccard distance
'''
# jaccard_dist_list = []
# cnt = 0
# base = 1
# for i in xrange (len(data)):
# 	for j in xrange (i+1, len(data)):
# 		cnt += 1
# 		if (cnt == base):
# 			print cnt
# 			base = base * 2
# 		if (not is_same_device (data[i], data[j])):
# 			jaccard_dist_list.append (cal_jaccard (data[i], data[j]))


'''
Plot histogram
'''
dist_hist = get_dist_hist (jaccard_dist_list, bucket_num = 10)
plt.figure()
plt.plot(dist_hist, 'yo', color='red')
# plt.xscale('log')
# plt.yscale('log')
plt.xlabel('jaccard distance bucket')
plt.ylabel('number of records')
#plt.axis([10, 1000, -100, 10000])
# plt.legend()
plt.title('with-device jaccard istance distribution of SF')
plt.savefig(os.path.join(path, 'within_device_jaccard_dist_hist.png'))

# write_csv ('../data/interest_records.csv', record_list)