import sys, os, csv

'''
   
   This script is used to transform data file into readable csv format
   using FILTER_IDX, we can extract only interesting collumns

'''
def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)


data_file = 'wrong_device'
output_file = 'wrong_device.csv'
output_data = []
with open (data_file) as f:
    for line in f:
        line = line.strip ()
        if (not line):
            continue
        record_list = line.split('|')
        for record in record_list:
            attr_list = record.split (',')
            output_data.append (attr_list)
 
write_csv (output_file, output_data)
print len (output_data)