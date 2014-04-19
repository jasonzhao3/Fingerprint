import sys, os, csv
from common.helper import write_csv
from config.data_config import DATA_PATH as path

'''
  Parse one beacon data file and extract all US_CA records, and dump into a csv file;
  This script can be used to filter records based on any attribute
'''

data_file_name = 'njprod-beacon-201_beacon.log.2014-04-06'
# data_file_name = 'small.txt'
output_file_name = 'us_ca_sf_data.csv'


data_file = os.path.join (path, data_file_name)
output_file = os.path.join (path, output_file_name)


# country: 29
# state: 30  US_CA
# entry point: 38
output_data = []

with open (data_file) as f:
    for line in f:
        line = line.strip ()
        attr_list = line.split (',')
        print len (attr_list)
        if (len(attr_list) == 60 
            and attr_list[30] == 'US_CA'
            and attr_list[31] == 'San Francisco'):
       		 # and attr_list[38] == '2'):
            output_data.append(attr_list)

 
write_csv (output_file, output_data)
print len (output_data)
