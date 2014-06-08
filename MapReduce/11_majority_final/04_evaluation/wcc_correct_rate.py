from __future__ import division
import os
from os import listdir

tot_cnt = 0
correct_cnt = 0
user_cnt = 0
with open('wcc', 'r') as f:
	for line in f:
		line = line.strip()
		flag = line.split('|')[1]
		users = line.split('\t')[1].split(',')
		user_cnt += len(users)
		tot_cnt += 1
		if (flag == 'True'):
			correct_cnt += 1

print tot_cnt, correct_cnt, user_cnt, correct_cnt/tot_cnt
