#!/usr/bin/env python

import sys, csv, math, os
# sys.path.append('./')
# sys.path.append(os.path.dirname(__file__))
# sys.path.append("./")

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


'''
  build geo map
'''
geo_map = build_geo_map ('US-City-Location.csv')
# geo_map = build_geo_map ('../../../local_data/rb_lsh_week/US-City-Location.csv')
geo_threshold = 0.02

total_cnt = 0
errors = [0] * 10
threshold_list = [0.5, 0.6, 0.7, 0.75, 0.8, 0.83, 0.86, 0.9, 0.93, 0.95]
for line in sys.stdin:
      total_cnt += 1
      line = line.strip ()
      city1 = line.split('_')[1]
      city2 = line.split('_')[2]
      record = line.replace('\t', '_').replace(',', '_').split('_')
      sim = float(record[2])

      dist_sqr = cal_geo_dist_sqr (city1, city2, geo_map)
      if (dist_sqr > geo_threshold):
        if (sim >= 0.95):
          errors[9] += 1
          # print '%s%s%s' % (0.95, "\t", line)
        if (sim >= 0.93):
          errors[8] += 1
          # print '%s%s%s' % (0.93, "\t", line)
        if (sim >= 0.9):
          errors[7] += 1
          # print '%s%s%s' % (0.9, "\t", line)
        if (sim >= 0.86):
          errors[6] += 1
          # print '%s%s%s' % (0.86, "\t", line)
        if (sim >= 0.83):
          errors[5] += 1
          # print '%s%s%s' % (0.83, "\t", line)
        if (sim >= 0.8):
          errors[4] += 1
          # print '%s%s%s' % (0.8, "\t", line)
        if (sim >= 0.75):
          errors[3] += 1
          # print '%s%s%s' % (0.75, "\t", line)
        if (sim >= 0.7):
          errors[2] += 1
          # print '%s%s%s' % (0.7, "\t", line)
        if (sim >= 0.6):
          errors[1] += 1
          # print '%s%s%s' % (0.6, "\t", line)
        if (sim >= 0.5):
          errors[0] += 1
          # print '%s%s%s' % (0.5, "\t", line)

for idx, num_error in enumerate(errors):
  print '%s%s%s' % (str(threshold_list[idx]), "\t", str(num_error))

print '%s%s%s' % ('0', '\t', str(total_cnt))



