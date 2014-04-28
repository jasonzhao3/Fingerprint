import sys, os
# from config.data_config import LOCAL_DATA_PATH as path
from collections import Counter
import pylab as plt
from common.helper import *
from common.similarity_helper import *
import numpy as np


'''
	dataset description
	===================
	dataset: part00000, part00001, part00002
	total device #: 1762 + 1732 + 1772 = 5266
	total pair #: 13862745

	test whether lsh works:
	==================
	threshold = 0.8
	ground_truth   correct   total     
	4944		   4944     2050805   
	So, 
	false positive: a lot
	false negative: 0

	==================
	threshold = 0.7
	ground_truth   correct  total  
	13150          13145    205805
	so,
	false positive: as long as we threshold the lsh result, very small
	false negative: 5 / 13150 = 0.0003

	==================
	threshold = 0.6
	ground_truth   correct  total
	344515 			344249 	344249 0.999227900091		
	so,
	false postive: same as above
	false negative: 266 / 344515 = 0.0007

	=================
	threshold = 0.5
	ground_truth  correct  total 
	675332		  663861 	663861 
	So,
	false positive: as usual
	false negative: 14471 / 675332 = 0.01698
	
'''



path = '../../local_data/join_request_beacon/'
data_file_names = ["part012_sim_lsh_result_0.5", "part012_sim_result_0.5"]
output_file_name = "lsh_eval_result"

file_lsh = os.path.join (path, data_file_names[0])
file_ground = os.path.join (path, data_file_names[1])


ground_truth = set()
with open (file_ground) as f:
	for line in f:
		line = line.strip ()
		key = line.split ('\t')[0]
		# print key
		# ground_map[key] = float(line.split('\t')[1])
		ground_truth.add (key)

print len (ground_truth)

match_cnt = 0
total_cnt = 0
with open (file_lsh) as f:
	for line in f: 
		total_cnt += 1
		line = line.strip ()
		key = line.split('\t')[0]
		if key in ground_truth:
			match_cnt += 1
			# print match_cnt

print len (ground_truth), match_cnt, total_cnt, float(match_cnt) / len (ground_truth)
