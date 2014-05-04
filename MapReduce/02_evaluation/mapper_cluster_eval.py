#!/usr/bin/env python

import sys, csv, math, os
# sys.path.append('./')
# sys.path.append(os.path.dirname(__file__))
sys.path.append("./")

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
  print len(geo_map)
  return geo_map


def cal_geo_dist_sqr (city1, city2, geo_map):
  loc1 = geo_map[city1]
  loc2 = geo_map[city2]
  dist_sqr = math.pow((loc1[0] - loc2[0]), 2) + math.pow((loc2[1] - loc2[1]), 2)
  return dist_sqr

def get_error_cnt (city_list, geo_threshold):
  num = 0
  denum = 0
  for i in xrange (len(city_list)):
    for j in xrange (i+1, len(city_list)):
      denum += 1
      dist_sqr = cal_geo_dist_sqr (city_list[i], city_list[j], geo_map)
      if (dist_sqr > geo_threshold):
        num += 1
  return num, denum



'''
  build geo map
'''
geo_map = build_geo_map ('US-City-Location.csv')
# geo_map = build_geo_map ('../../../local_data/rb_lsh_week/US-City-Location.csv')
geo_threshold = 0.02

total_cnt = 0
error_cnt = 0
for line in sys.stdin:
      line = line.strip ()
      # get rid of empty line
      if (not line):
        continue
      profile_list = line.split ('|')
      city_list = [item[1] for item in profile_list];
      num, denum = get_error_cnt (city_list, geo_threshold)
      total_cnt += denum
      error_cnt += num
            
threshold = 0.8
print '%s%s%s' % (str(threshold), "\t", ",".join(str(error_cnt), str(total_cnt)));

# output: 0.8 10,18


