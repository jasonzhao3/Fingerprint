#!/usr/bin/env python
'''
    This map-reduce job takes in 3 Terabytes Yume Request Data,
    Output only CA request data with selected features

    Mapper Output Format:
    identifier  \t  feature_list
'''
import sys

STATE_IND = 24
COOKIE_IND = 44

FILTER_IDX = [
        0, # session_id 
        4, # domain_id - majority
        67, # placment_id
        66, # advertisement_id
        43, # requested_date - frequency within 4 hours
        26, # census_DMA - majority
        25, # city_name - jaccard set
        2, # publisher_id - set
        20, # content_video_id (skip 0)
        31, # delivery_point_id
        52, # service_provider_id - jaccard set
        39, # key_value - jaccard set
        55, # player_location_id 
        54, # player_size_id - jaccard set
        56, # page_fold_id - majority
        57, # ad_visibility
        60, # ovp_version  
        59, # ovp_type
        62, # hid
        64, # is_on_premise
        65, # audience_segment


        1, # referrer_site (skip NULL)
        3, # network_id - set
        6, # slot_type_id - majority (low weight because too many 1)
        7, # ad_request_id
        12, # is_not_yume_white_list  - ratio of true
        18, # publisher_channel_id - (skip 0)
        21, # content_video_identifier (skip null)
        22, # content_profile_id (skip null)
        28, # is_pre_fetch_request
        38, # service_provider_name  - majority
        44, # cookie_identifier
        45 # ip_addr - jaccard set
        68, # behavior_cookie (skip NULL)

]



# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    attr_list = line.split('|')
    if len(attr_list) == 71:
        cookie = attr_list[COOKIE_IND]
        state = attr_list[STATE_IND]
        if state == "US_CA":
            filter_list = [attr_list[i] for i in FILTER_IDX]
            fields = cookie.split('_')
            if (len(fields) == 5):
                identifier = fields[4]
                filter_list.append(fields[0] + "." + fields[1] + "." + fields[2] + "." + fields[3])
            elif (len(fields) == 1):
                identifier = "null"
                filter_list.extend(fields)
            print '%s%s%s' % (identifier, "\t", ','.join(filter_list))

