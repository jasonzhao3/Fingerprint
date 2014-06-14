from __future__ import division
import os
from os import listdir
from collections import Counter

tot_user_cnt = 0
correct_cnt = 0
tot_device_cnt = 0
tot_delivery_correct_cnt = 0
user_cnt = [0]*7



with open('./wcc/wcc_mix_v10.2_t0.83', 'r') as f:
	for line in f:
		tot_user_cnt += 1
		line = line.strip()
		flag = line.split('|')[1]
		devices = line.split('|')[0].split('\t')[1].split(',')
		num_device = len(devices)
		tot_device_cnt += num_device
		
		delivery_counter = Counter()
		is_wrong_delivery = False
		for device in devices:
			delivery_point = device.split('_')[-1]
			delivery_counter[delivery_point] += 1;
			
#		if (delivery_counter['1'] > 2 and delivery_counter['1'] < 5):
#			is_wrong_delivery = True

		if (delivery_counter['2'] > 1 or delivery_counter['3']):
			is_wrong_delivery = True

		if not is_wrong_delivery:
			tot_delivery_correct_cnt += 1
		
		if (num_device > 7):
			num_device = 7
		user_cnt[num_device-1] += 1
		
		if (flag == 'True'):
			correct_cnt += 1

print tot_user_cnt, correct_cnt, correct_cnt/tot_user_cnt
print user_cnt
print tot_device_cnt
print tot_delivery_correct_cnt, tot_delivery_correct_cnt/tot_user_cnt




'''
	Evaluate the error rate of different device type
'''
