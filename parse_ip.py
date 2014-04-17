from freegeoip import get_geodata
import csv, os

def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)

def csv_reader (csv_file):
	data = []
	with open(csv_file, "rb") as f:
		reader = csv.reader(f)
		for row in reader:
			data.append (row)
	return data

def get_ip_map (data):
	ip_map = {}
	for record in data:
		cookie = record[22];
		attr_list = cookie.split('_')
		
		if (len(attr_list) == 5):
			ip_addr = attr_list[0] + '.' + attr_list[1] + '.' + attr_list[2] + '.' + attr_list[3]
			time = record[16]
			if (ip_addr in ip_map):
				ip_map[ip_addr].append (time)
			else:
				ip_list = []
				ip_list.append (time)
				ip_map[ip_addr] = ip_list
		else:
			print attr_list
			data.remove (record)
	return ip_map


path = '../data'
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


