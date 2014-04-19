import sys, os, csv
import json

# 44 is cookie => need parse it in mapper
filter_idx = [
1, 2, 3, 4,
6,
9, 10, 11, 12, 13, 14, 15, 16,
18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
43, 
44, 
45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 
60, 61, 62, 63, 64, 65, 66, 67, 68, 69
]



schema = [
        "session_id",
        "referrer_site",
        "publisher_id",
        "network_id",
        "domain_id",
        "domain_identifier",
        "slot_type_id",
        "ad_request_count",
        "ad_served_count",
        "is_yume_blacklisted",
        "is_network_blacklisted",
        "is_not_in_network_white_listed",
        "is_not_yume_whitelisted",
        "is_unknown_domain_for_yume",
        "is_unknown_domain_for_in_network",
        "ad_visibility_violation",
        "ad_duration_violation",
        "un_paid_ad_count",
        "publisher_channel_id",
        "publisher_page_id",
        "content_video_id",
        "content_video_identifier",
        "content_profile_id",
        "country_code",
        "state_code",
        "city_name",
        "census_data_DMA_id",
        "is_wthout_pre_fetch",
        "is_pre_fetch_request",
        "published_player_identifier",
        "content_playlist_identifier",
        "delivery_point_id",
        "device_make_name",
        "device_model_name",
        "os_name",
        "os_version_name",
        "browser_name",
        "browser_version_name",
        "service_provider_name",
        "key_values",
        "sdk_version",
        "back_fill",
        "session_id_dup",
        "requested_date",
        "cookie",
        "ip_address",
        "device_make_id",
        "device_model_id",
        "os_id",
        "os_version_id",
        "browser_id",
        "browser_version_id",
        "service_provider_id",
        "network_key_value",
        "player_size_id",
        "player_location_id",
        "page_fold_id",
        "ad_visibility_id",
        "play_type",
        "ovp_type",
        "ovp_version",
        "ovp_plugin",
        "hid",
        "num_of_devices",
        "is_on_premises",
        "audience_segments",
        "advertisement_id",
        "placement_id",
        "behaviour_cookie",
        "remarketing_cookie",
        "json_data"
]

def write_csv (csv_file, list_of_list):
    with open(csv_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_list)

'''
  input and output setting
'''

path = '../local_data'
data_file_name = 'small_request'
# data_file_name = 'small.txt'
# schema_file_name = 'schema'
output_file_name = 'select_request_small.csv'


data_file = os.path.join (path, data_file_name)
# schema_file = os.path.join (path, schema_file_name)
output_file = os.path.join (path, output_file_name)


# schema_map = create_schema (schema_file)
#print schema_map

#country: 29
#state: 30  US_CA

output_data = []
select_schema = [schema[i] for i in filter_idx]
output_data.append (select_schema)

with open (data_file) as f:
    for line in f:
        line = line.strip ()
        attr_list = line.split ('|')
        select_list = [attr_list[i] for i in filter_idx]
        output_data.append (select_list)
        # print len (attr_list)
        # if (len(attr_list) == 60 
        #     and attr_list[30] == 'US_CA'
        #     and attr_list[31] == 'San Francisco'):
        #     output_data.append(attr_list)

 
write_csv (output_file, output_data)
print len (output_data)