import os

DATA_PATH = '../local_data/'
LOCAL_DATA_PATH = '../../local_data'

# schema for request data  -- 71 attributes
REQUEST_SCHEMA = [
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


'''
Idx preserved for useful attributes, 44 is cookie => need parse it in mapper
'''
FILTER_IDX = [
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

''' 
   New schema of the useful attributes
'''
NEW_SCHEMA = [
        'referrer_site', 
        'publisher_id', 
        'network_id', 
        'domain_id', 
        'slot_type_id', 
        'is_yume_blacklisted', 
        'is_network_blacklisted', 
        'is_not_in_network_white_listed', 
        'is_not_yume_whitelisted', 
        'is_unknown_domain_for_yume', 
        'is_unknown_domain_for_in_network', 
        'ad_visibility_violation', 
        'ad_duration_violation', 
        'publisher_channel_id', 
        'publisher_page_id', 
        'content_video_id', 
        'content_video_identifier', 
        'content_profile_id', 
        'country_code', 
        'state_code', 
        'city_name', 
        'census_data_DMA_id', 
        'is_wthout_pre_fetch', 
        'is_pre_fetch_request', 
        'delivery_point_id', 
        'device_make_name', 
        'device_model_name', 
        'os_name', 
        'os_version_name', 
        'browser_name', 
        'browser_version_name', 
        'service_provider_name', 
        'key_values', 
        'sdk_version', 
        'back_fill', 
        'requested_date', 
        'ip_address', 
        'device_make_id', 
        'device_model_id', 
        'os_id', 
        'os_version_id', 
        'browser_id', 
        'browser_version_id', 
        'service_provider_id', 
        'network_key_value',
        'player_size_id', 
        'player_location_id', 
        'page_fold_id', 
        'ad_visibility_id', 
        'play_type', 
        'ovp_type', 
        'ovp_version', 
        'ovp_plugin', 
        'hid', 
        'num_of_devices', 
        'is_on_premises', 
        'audience_segments', 
        'advertisement_id', 
        'placement_id', 
        'behaviour_cookie', 
        'remarketing_cookie',
        'ip_address_2',
        'identifier']

'''
profile Idx
'''

PROFILE_IDX = [
        1, # publisher_id - majority
        2, # network_id - majority
        3, # domain_id - majority
        8, # is_not_yume_white_list  - ratio of true
        20, # city_name - jaccard set
        21, # census_DMA - majority
        31, # service_provider_name  - majority
        32, # key_value - jaccard set
        35, # requested_date - frequency within 4 hours
        36, # ip_addr - jaccard set
        43, # service_provider - jaccard set
        45, # player_size - jaccard set
        47, # page_fold - majority
        49, # play_type - majority
        53, # hid - majority
        55, # is_on_premises - 1's ratio
]