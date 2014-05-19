#!/usr/bin/env python

'''
    This map-reduce job takes in 30G Yume Beacon Data,
    Output only CA beacon data with selected features

    Mapper Output Format:
    identifier  \t  feature_list
'''
import sys

STATE_IND = 30
VIEWER_IND = 22
BEACON_FILTER_IDX = [
    0, # session id    
    1, # domain id      
    3, # placement_id
    4, # advertisement_id
    16, # event time
    28, # census_dma_id
    31, # city
    32, # publisher_channel_id
    34, # content_video_id
    38, # delivery point
    45, # service provider id
    46, # key values 
    48, # player location 
    49, # player size
    50, # page fold
    51, # ad visibility
    53, # ovp version
    54, # ovp type
    55, # hid
    57, # is on-premise
    58, # audience segments
    ############################
    18, # slate id
    ############################     
    5, # zero_tracker
    6, # twentry_five
    7, # fifty
    8, # seventry_five
    9, # one_hundred
    27, # volume percent
    #10, # click_tracker
    #11, # customization_id
    #12, # custom_type_id
    #13, # custom_report_id
    #14, # custom_event_id
    #15, # is_media_buy
    #16, # request_time
    
    #27, # volume_percent
    #30, # state
    #31, # city
    #48, # player_location
    #49  # player_size
]

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    attr_list = line.split(',')
    if len(attr_list) == 60:
        cookie = attr_list[VIEWER_IND]
        fields = cookie.split('_')
        if (len(fields) == 5):
            identifier = fields[4]
        state = attr_list[STATE_IND]
        if state == "US_CA":
            filter_list = [attr_list[i] for i in BEACON_FILTER_IDX if i != VIEWER_IND and i != STATE_IND]
            print '%s%s%s' % (identifier, "\t", ','.join(filter_list))

