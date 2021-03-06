#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys, csv, math, os
 # sys.path.append(os.path.dirname(__file__))
sys.path.append("./")

'''
  Evaluation: use president election method
  as long as one pair is wrong, the user is wrong
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

def get_error_cnt (city_list, geo_threshold):
  if (len(city_list) == 1):
    return 0, 1
  num = 0
  denum = 0
  for i in xrange (len(city_list)):
    for j in xrange (i+1, len(city_list)):
      denum += 1
      dist_sqr = cal_geo_dist_sqr (city_list[i], city_list[j], geo_map)
      if (dist_sqr > geo_threshold):
        num += 1
  return num, denum

def is_error_user (city_list, geo_threshold):
  if (len(city_list) == 1):
    return False
  else:
    for i in xrange (len(city_list)):
      for j in xrange (i+1, len(city_list)):
        dist_sqr = cal_geo_dist_sqr (city_list[i], city_list[j], geo_map)
        if (dist_sqr > geo_threshold):
          return True
  return False



'''
  build geo map
'''
geo_map = build_geo_map ('US-City-Location.csv')
# geo_map = build_geo_map ('../../../../local_data/GeoLiteCity_20140401/US-City-Location.csv')
geo_threshold = 0.02

# error_threshold = 0.1


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    
    # key is the bucket group
    for key, group in groupby(data, itemgetter(0)):
        try:
            error_cnt = 0; user_cnt = 0; device_cnt = 0
            for cluster_idx, cities in group:
                user_cnt += 1
                city_list = cities.split(',')
                device_cnt += len(city_list)
                if (is_error_user(city_list, geo_threshold)):
                  error_cnt += 1

                # tmp_error_cnt, tmp_tot_cnt = get_error_cnt (city_list, geo_threshold)
                # if (tmp_error_cnt > 0):
                #   error_cnt += 1
                # # if (float(tmp_error_cnt) / tmp_tot_cnt > error_threshold):
                #   error_cnt += 1
                
            print "%s%s%s" % (key, separator, str(error_cnt)+','+str(user_cnt)+','+str(device_cnt))
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()

    