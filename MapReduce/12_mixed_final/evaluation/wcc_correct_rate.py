from __future__ import division
import os
from os import listdir

tot_cnt = 0
correct_cnt = 0
tot_device_cnt = 0
tot_delivery_correct_cnt = 0
user_cnt = [0]*7

with open('wcc_new', 'r') as f:
	for line in f:
		tot_cnt += 1
		line = line.strip()
		flag = line.split('|')[1]
		devices = line.split('|')[0].split('\t')[1].split(',')
		num_device = len(devices)
		tot_device_cnt += num_device
		deliveryMap = dict()
		is_wrong_delivery = False
		for device in devices:
			deliveryPoints = device.split('_')
			deliveryPoint = deliveryPoints[len(deliveryPoints)-1]
			if deliveryPoint not in deliveryMap:
				deliveryMap[deliveryPoint] = 0;
			deliveryMap[deliveryPoint] += 1;
			if deliveryMap[deliveryPoint] > 2:
				is_wrong_delivery = True
				break
		if not is_wrong_delivery:
			tot_delivery_correct_cnt += 1
		if (num_device > 7):
			num_device = 7
		user_cnt[num_device-1] += 1
		
		if (flag == 'True'):
			correct_cnt += 1

print tot_cnt, correct_cnt, correct_cnt/tot_cnt
print user_cnt
print tot_device_cnt
print tot_delivery_correct_cnt, tot_delivery_correct_cnt/tot_cnt




'''
	Evaluate the error rate of different device type
'''
