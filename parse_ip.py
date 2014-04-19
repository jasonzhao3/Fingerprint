from common.freegeoip import get_geodata
import csv, os
from common.helper import write_csv, csv_reader, get_identifier_and_ip
from config.data_config import DATA_PATH as path

'''
 
 	Based on ip-to-geo translation, this script parses each record into form:
 	['lat', 'long', 'timestamp'], and group records from same location together
 
 '''

# record[16] is time field
def get_ip_map (data):
	ip_map = {}
	for record in data:
		identifier, ip_addr = get_identifier_and_ip (record[22])

		if (ip_addr != "null"):
			if (ip_addr in ip_map):
				ip_map[ip_addr].append (record[16])
			else:
				ip_list = []
				ip_list.append (record[16])
				ip_map[ip_addr] = ip_list
		else:
			print "invalid ip address"
			data.remove (record)
	return ip_map


data_file_name = 'us_ca_sf_data.csv'
data_file = os.path.join (path, data_file_name)
data = csv_reader (data_file)

ip_map = get_ip_map (data)
geo_list = []
geo_list.append (['Lat', 'Long', 'Name'])

cnt = 0
for ip in ip_map.keys():
	if (cnt > 10):
		break
	ip_json = get_geodata(ip)
	print ip_json
	curr_geo = []
	curr_geo.append (ip_json['latitude'])
	curr_geo.append (ip_json['longitude'])
	curr_geo.append ('; '.join (ip_map[ip]))
	geo_list.append (curr_geo)
	cnt += 1

print geo_list
write_csv ('../data/sf_geo_records.csv', geo_list)


