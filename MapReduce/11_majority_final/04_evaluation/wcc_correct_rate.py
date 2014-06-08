from __future__ import division
import os
from os import listdir

tot_cnt = 0
correct_cnt = 0
tot_device_cnt = 0
user_cnt = [0]*7

with open('wcc', 'r') as f:
	for line in f:
		line = line.strip()
		flag = line.split('|')[1]
		devices = line.split('\t')[1].split(',')
		
		num_device = len(devices)
		tot_device_cnt += num_device
		
		if (num_device > 7):
			num_device = 7
		user_cnt[num_device-1] += 1
		
		if (flag == 'True'):
			correct_cnt += 1

print tot_cnt, correct_cnt, correct_cnt/tot_cnt
print user_cnt
print tot_device_cnt




'''
	Evaluate the error rate of different device type
'''
