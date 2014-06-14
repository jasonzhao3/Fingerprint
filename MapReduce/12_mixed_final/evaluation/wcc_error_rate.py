from __future__ import division
import os
from os import listdir
from collections import Counter

tot_user_cnt = 0  # total non-single user count
error_user_cnt = 0
tot_device_cnt = 0 # total non-single user device count

user_cnt = [0]*7  # 7 types of user
tl_error_user_cnt = [0]*7
dp_error_user_cnt = [0]*7



with open('./wcc/wcc_mix_v10.2_t0.83', 'r') as f:
	for line in f:
		tot_user_cnt += 1
		
		line = line.strip()
		flag = line.split('|')[1]
		devices = line.split('|')[0].split('\t')[1].split(',')
		num_device = len(devices)
		tot_device_cnt += num_device
		
		if (num_device > 7):
			num_device = 7

		user_cnt[num_device-1] += 1
		
# delivery point evaluation
		delivery_counter = Counter()
		is_wrong_delivery = False
		for device in devices:
			delivery_point = device.split('_')[-1]
			delivery_counter[delivery_point] += 1;

#		if (delivery_counter['1'] > 2 and delivery_counter['1'] < 5):
#			is_wrong_delivery = True

		if (delivery_counter['2'] > 1 or delivery_counter['3'] > 1):
			is_wrong_delivery = True

		if is_wrong_delivery:
			dp_error_user_cnt[num_device-1] += 1

# time location evaluation
		if (flag != 'True'):
			tl_error_user_cnt[num_device-1] += 1


tot_tl_error = sum(tl_error_user_cnt)
tot_dp_error = sum(dp_error_user_cnt)

print "total user count %d" % tot_user_cnt
print "total device count %d" % tot_device_cnt
print "user of each type count: ", user_cnt
print "\n"

print "time location evaluation error user #:", tl_error_user_cnt
print "total time location error user %d" % tot_tl_error
print "time location error ratio of each type of user: ", [tl_error_user_cnt[i] / user_cnt[i] for i in xrange(1, 7)]
print "total time location error rate: ", tot_tl_error / tot_user_cnt
print "\n"


print "delivery point evaluation error user #: ", dp_error_user_cnt
print "total delivery_point error user %d" % tot_dp_error
print "deliver_point error ratio of each type of user: ", [dp_error_user_cnt[i] / user_cnt[i] for i in xrange(1, 7)]
print "total delivery point error rate: ", tot_dp_error / tot_user_cnt

