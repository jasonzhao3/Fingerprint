#!/usr/bin/env python

import sys


'''
	LSH hash function
'''
NUM_BUCKET = 1000
# strong hash to 1000 buckets
def hash_bucket_1 (publisher_id, network_id, domain_id):
	# publisher_id = int (publisher_id)
	# network_id = int (network_id)
	# domain_id = int (domain_id)
	return ((17 * publisher_id) + (13 * network_id) + (3 * domain_id)) % NUM_BUCKET

# tune the parameter from 17,13 to 13,7
def hash_bucket_2 (dma, hid, service_provider):
	return ((13 * dma) + (7 * hid) + service_provider) % NUM_BUCKET

# hash service_provide_name -- based on dan bernstein in comp.lang.
def hash_string (input_str):
    djb2_code = 5381
    for i in xrange (0, len (input_str)):
        char = input_str[i];
        djb2_code = (djb2_code << 5) + djb2_code + ord (char)
    return djb2_code % 1000

# profile is an attribute list
def get_hash_buckets (profile):
  if (profile[0].isdigit ()):
    publisher_id = int (profile[0])
  else:
    publisher_id = 3333 #heuristic value for NA
  
  if (profile[1].isdigit ()):
    network_id = int (profile[1])
  else:
    network_id = 3333 

  if (profile[2].isdigit ()):
    domain_id = int (profile[2])
  else:
    domain_id = 3333
  
  if (profile[5].isdigit ()):
    dma = int (profile[5])
  else:
    dma = 3333

  if (profile[13].isdigit ()):
    hid = int (profile[13])
  else:
    hid = 3333
    
  service_provider = hash_string (profile[6])

  bucket1 = hash_bucket_1 (publisher_id, network_id, domain_id)
  bucket2 = hash_bucket_2 (dma, hid, service_provider)
  return bucket1, bucket2


PROFILE_IDX = [
               1, # publisher_id - majority
               2, # network_id - majority
               3, # domain_id - majority
               8, # is_not_yume_white_list  - ratio of true
               20, # city_name - jaccard set
               21, # census_DMA - majority
               31, # service_provider_name  - majority
               32, # key_value - jaccard set
               #35, # requested_date - frequency within 4 hours
               36, # ip_addr - jaccard set
               43, # service_provider - jaccard set
               45, # player_size - jaccard set
               47, # page_fold - majority
               49, # play_type - majority
               53, # hid - majority
               55, # is_on_premises - 1's ratio
               ];

attr_num = len(PROFILE_IDX);
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    # split the line into words
    record_list = l[1].split('|')
    if len(record_list) >= 30:
      attr_count = []
      for i in range(0, attr_num):
          attr_count.append([])
      for record in record_list:
          attr_l = record.split(',')
          attr_list = [attr_l[i] for i in PROFILE_IDX]
          for i in range(0, len(attr_list)):
            attr_count[i].append(attr_list[i])
      attr_res = []
      for attrs in attr_count:
        m = dict()
        for attr in attrs:
            if attr not in m:
                m[attr]  = 0
            m[attr] += 1
        maxNum = 0
        maxItem = ''
        for key,value in m.items():
            if (value > maxNum):
                maxNum = value;
                maxItem = key
        attr_res.append(maxItem)
      basket1, basket2 = get_hash_buckets (attr_res)
      attr_res.append(l[0])
      print '%s%s%s' % (str(basket1)+"_1", "\t", ','.join(attr_res))
      print '%s%s%s' % (str(basket2)+"_2", "\t", ','.join(attr_res))

