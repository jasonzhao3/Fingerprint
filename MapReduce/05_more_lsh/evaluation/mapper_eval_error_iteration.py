#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys, csv, math, os

sys.path.append("./")


'''
  Input Format:
  bucket_idx_group \t device_profile

  There may be multiple lines of devices belong to the same bucket

  The corresponding is still reducer_eval_error.py
'''


'''
  Helper functions
'''
def read_csv (csv_file):
  data = []
  with open(csv_file, "rb") as f:
    reader = csv.reader(f)
    for row in reader:
      data.append (row)
  return data


def build_geo_map(location_file):
  # build geo location map
  data = read_csv (location_file)
  geo_map = {}
  for record in data[1:]:
    geo_map[record[3]] = (float(record[5]), float(record[6]))
  # print len(geo_map)
  return geo_map


def cal_geo_dist_sqr (city1, city2, geo_map):
  loc1 = geo_map[city1]
  loc2 = geo_map[city2]
  dist_sqr = math.pow((loc1[0] - loc2[0]), 2) + math.pow((loc2[1] - loc2[1]), 2)
  return dist_sqr

def cut_to_set(ts_list):
  ts_list = [ts[:-7] for ts in ts_list]
  return set(ts_list)

# # as long as they are more than 25 miles away, fail
# def fail_location(dev1, dev2):
#   city_list1 = dev1[CITY_IDX].split('|')
#   city_list2 = dev2[CITY_IDX].split('|')

#   for city1 in city_list1:
#     for city2 in city_list2:
#       try:
#         dist = cal_geo_dist_sqr(city1, city2, geo_map)
#         if (dist > geo_threshold):
#           return True
#       except (RuntimeError, TypeError, NameError, KeyError, IOError):
#           pass
#   return False

# only if all pair are more than 25 miles away, fail
def fail_location(dev1, dev2):
  city_list1 = dev1[CITY_IDX].split('|')
  city_list2 = dev2[CITY_IDX].split('|')

  for city1 in city_list1:
    for city2 in city_list2:
      try:
        dist = cal_geo_dist_sqr(city1, city2, geo_map)
        if (dist <= geo_threshold):
          return False
      except (RuntimeError, TypeError, NameError, KeyError, IOError):
          pass
  return True

# as long as they share one timestamp, fail
def fail_timestamp(dev1, dev2):
  ts1 = dev1[TS_IDX].split('|')
  ts2 = dev2[TS_IDX].split('|')
  ts1_set = cut_to_set(ts1)
  ts2_set = cut_to_set(ts2)
  inter_set = ts1_set & ts2_set
  if (len(inter_set) != 0):
    return True
  else:
    return False

def fail_hid(dev1, dev2):
  hid1 = set(dev1[HID_IDX].split('|'))
  hid2 = set(dev2[HID_IDX].split('|'))
  inter_set = hid1 & hid2
  if (len(inter_set) == 0):
    return True
  else:
    return False


# success return True, otherwise return False
def eval_cluster(cluster):
  for idx1, dev1 in enumerate(cluster):
    for idx2 in xrange(idx1+1, len(cluster)):
      dev2 = cluster[idx2]
      if (fail_timestamp(dev1, dev2) or
          fail_hid(dev1, dev2) or 
          fail_location(dev1, dev2)):
        return False
  return True

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


'''
  build geo map
'''
geo_map = build_geo_map ('US-City-Location.csv')
# geo_map = build_geo_map ('../../US-City-Location.csv')

TS_IDX = 3
CITY_IDX = 5
HID_IDX = 17


geo_threshold = 0.02

 
def main(separator='\t'):
     # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    for key, group in groupby(data, itemgetter(0)):
        try:
            cluster = [];
            for key, device in group:
              cluster.append (device.split(','))
            # print cluster
            is_correct = eval_cluster(cluster)
            key_suffix = key.split('_')[1]
            if (is_correct):
              print ("%s%s%d" % ("correct" + '_' + key_suffix, '\t', 1))
            else:
              print ("%s%s%d" % ("wrong" + '_' + key_suffix, '\t', 1)) 

        except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
            print "ERROR!!"
            pass

if __name__ == "__main__":
    main()
