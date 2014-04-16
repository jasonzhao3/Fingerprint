import sys, os, csv
import json

def create_schema (schema_file):
    schema_map = {}
    with open (schema_file) as f:
        for line_num, line in enumerate(f):
            line = line.strip ()
            attr_list = line.split ('#')
            attr_name = attr_list[0].split(' ')[0]
            schema_map[attr_name] = line_num
    return schema_map

def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)

'''
  input and output setting
'''

path = '../cs341_2014/data'
data_file_name = 'njprod-beacon-201_beacon.log.2014-04-06'
# data_file_name = 'small.txt'
schema_file_name = 'schema'
output_file_name = 'us_data.csv'


data_file = os.path.join (path, data_file_name)
schema_file = os.path.join (path, schema_file_name)
output_file = os.path.join (path, output_file_name)


schema_map = create_schema (schema_file)
#print schema_map

output_data = []
with open (data_file) as f:
    for line in f:
        line = line.strip ()
        attr_list = line.split (',')
        print len (attr_list)
        if (len(attr_list) == 60):
            output_data.append(attr_list)

write_csv (output_file, output_data)
print len (output_data)





