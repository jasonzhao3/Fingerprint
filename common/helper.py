import csv


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
