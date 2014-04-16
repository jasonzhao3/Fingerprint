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

<<<<<<< HEAD:group_state2.py
path = '../../Downloads/'
data_file_name = 'njprod-beacon-201_beacon.log.2014-04-06'
# data_file_name = 'small.txt'
#schema_file_name = 'schema'
output_file_name = 'us_data.csv'


data_file = os.path.join (path, data_file_name)
#schema_file = os.path.join (path, schema_file_name)
output_file = os.path.join (path, output_file_name)


#schema_map = create_schema (schema_file)
=======
path = '../data'
data_file_name = 'njprod-beacon-201_beacon.log.2014-04-06'
# data_file_name = 'small.txt'
# schema_file_name = 'schema'
output_file_name = 'us_ca_sf_data.csv'


data_file = os.path.join (path, data_file_name)
# schema_file = os.path.join (path, schema_file_name)
output_file = os.path.join (path, output_file_name)


# schema_map = create_schema (schema_file)
>>>>>>> 14b8ceb5f712adcf55f5727e0384d667424e401e:group_state.py
#print schema_map

#country: 29
#state: 30  US_CA

output_data = []
cookie = []
ip_address = []
identifier = []
with open (data_file) as f:
    for line in f:
        line = line.strip ()
        attr_list = line.split (',')
<<<<<<< HEAD:group_state2.py
        if (len(attr_list) == 60):
            cookie.append(attr_list[22])
            temp = attr_list[22].split('_')
            #print(temp)
            #print len(output_data)
            if (len(temp) == 5):
                ip = temp[0] + "." + temp[1] + "." + temp[2] + "." + temp[3] 
                ip_address.append(ip)
                identifier.append(temp[-1])
            if (len(temp) == 1):
                ip_address.append(temp)
                identifier.append("null")
            attr_list.append(ip_address[-1])
            attr_list.append(identifier[-1])
            output_data.append(attr_list)

#print identifier
=======
        print len (attr_list)
        if (len(attr_list) == 60 
            and attr_list[30] == 'US_CA'
            and attr_list[31] == 'San Francisco'):
            output_data.append(attr_list)

 
>>>>>>> 14b8ceb5f712adcf55f5727e0384d667424e401e:group_state.py
write_csv (output_file, output_data)
print len (output_data)
