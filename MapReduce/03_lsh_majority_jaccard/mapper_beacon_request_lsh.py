#!/usr/bin/env python

import sys


'''
	LSH hash function
'''
NUM_BUCKET = 500

# hash service_provide_name -- based on dan bernstein in comp.lang.c
def hash_string (input_str):
    djb2_code = 5381
    for i in xrange (0, len (input_str)):
        char = input_str[i];
        djb2_code = (djb2_code << 5) + djb2_code + ord (char)
    return djb2_code % NUM_BUCKET


def clear_nan (profile):
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
  
  if (profile[3].isdigit ()):
    dma = int (profile[3])
  else:
    dma = 3333

  if (profile[7].isdigit ()):
    hid = int (profile[7])
  else:
    hid = 3333

  return publisher_id, network_id, domain_id, dma, hid


def hash_beacon_ptg (profile):
  ptg_0 = float (profile[-6])
  ptg_25 = float (profile[-5])
  ptg_50 = float (profile[-4])
  ptg_75 = float (profile[-3])
  ptg_100 = float (profile[-2])
  ptg_NA = float (profile[-1])

  return int (1024 * ptg_100 + 512 * ptg_75 + \
        256 * ptg_50 + 128 * ptg_25 + 64 * ptg_0 + 32 * ptg_NA) % NUM_BUCKET


# profile is an attribute list
# get request_beacon hash buckets
def get_rb_hash_buckets (profile):
  publisher_id, network_id, domain_id, dma, hid = clear_nan (profile)
  bucket1 = (publisher_id + network_id + domain_id) % NUM_BUCKET
  # profile[6]: service_provider_name
  bucket2 = (dma + hid + hash_string(profile[6])) % NUM_BUCKET
  bucket3 = hash_beacon_ptg (profile)

  return [bucket1, bucket2, bucket3]


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    l = line.split('\t')
    attr_list = l[1].split(',')
    basket1, basket2, basket3 = get_rb_hash_buckets(attr_list)
    attr_list.append(l[0])
    print '%s%s%s' % (str(basket1)+"_1", "\t", ','.join(attr_list))
    print '%s%s%s' % (str(basket2)+"_2", "\t", ','.join(attr_list))
    print '%s%s%s' % (str(basket3)+"_3", "\t", ','.join(attr_list))

