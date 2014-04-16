from __future__ import division
import csv, os
from collections import Counter
 
from itertools import groupby
from operator import itemgetter
import sys
 
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
        
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
    for identifier, group in groupby(data, itemgetter(0)):
        try:
            scores = []
            for identifier, feature1 in group:
                for feature2 in group:
                    if feature1 == feature2:
                        continue
                    else:
                        record1 = feature1.split(',')
                        record2 = feature2.split(',')
                        if len(record1) == len(record2):
                            scores.append(cal_jaccard(record1, record2))
            print ("%s%s%s" % (identifier, separator, ",".join(str(v) for v in scores)))           
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()
        
def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if record1[i] != "null" or record2[i] != "null":
            denom = denom +1
            if record1[i] == record2[i]:
                num = num + 1
    
    return num / denom
    

#device_map = {}
#ip_map = Counter ()
#
#
#for record in data:
#	cookie = record[22];
#	attr_list = cookie.split('_')
#	
#	if (len(attr_list) == 5):
#		ip_addr = attr_list[0] + '.' + attr_list[1] + '.' + attr_list[2] + '.' + attr_list[3]
#		ip_map[ip_addr] += 1 
#		if (attr_list[4] in device_map):
#			device_map[attr_list[4]].append (record)
#		else:
#			record_list = []
#			record_list.append (record)
#			device_map[attr_list[4]] = record_list
#	else:
#		print attr_list
#
#print len (data), len (device_map), len (ip_map)
#print device_map.values()[:10]
# print max(device_map.values()), max(ip_map.values())


# write_csv ('../data/interest_records.csv', record_list)