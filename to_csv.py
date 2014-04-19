import sys, os, csv
from config.data_config import REQUEST_SCHEMA
from config.data_config import DATA_PATH as path
from common.helper import read_csv, write_csv
'''
   
   This script is used to transform data file into readable csv format
   using FILTER_IDX, we can extract only interesting collumns

'''

data_file_name = 'small_plapp_201'
output_file_name = "small_plapp_201.csv"


data_file = os.path.join (path, data_file_name)
# schema_file = os.path.join (path, schema_file_name)
output_file = os.path.join (path, output_file_name)


output_data = []
# select_schema = [schema[i] for i in filter_idx]
# output_data.append (select_schema)
output_data.append (REQUEST_SCHEMA)

with open (data_file) as f:
    for line in f:
        line = line.strip ()
        attr_list = line.split ('|')
        # select_list = [attr_list[i] for i in filter_idx]
        # output_data.append (select_list)
        output_data.append (attr_list)
        # print len (attr_list)
        # if (len(attr_list) == 60 
        #     and attr_list[30] == 'US_CA'
        #     and attr_list[31] == 'San Francisco'):
        #     output_data.append(attr_list)

 
write_csv (output_file, output_data)
print len (output_data)