import csv
from time import strptime, mktime
import numpy as np


def cal_exact_match_ratio (profile1, profile2):
    if (len(profile1) != len(profile2)):
    	print profile1, profile2
    	print len(profile1), len(profile2)
        return None
    num = 0
    denom = 0
    for i in xrange(len(profile1)):
    	denom = denom +1
        if profile1[i] == profile2[i]:
        	num = num + 1
    return float(num) / denom

# manually add some approximate factor into matching
def cal_approx_match_ratio (profile1, profile2):
	if (len(profile1) != len(profile2)):
		return None
	num = 0
	denom = 0
	threshold = 0.01
	for i in xrange(len(profile1) - 6):
		denom += 1
		# request part -- exact match
		if (i < len(profile1) - 6) and (profile1[i] == profile2[i]):
			num += 1
		elif (i >= len(profile1) - 6):
			if (abs(float(profile1[i]) - float(profile2[i])) < threshold):
				num += 1
	return float(num) / denom


def tuple_to_key (str1, str2):
	if (str1 < str2):
		return (str1, str2)
	else:
		return (str2, str1)