#!/usr/bin/env python

import sys

STATE_IND = 30
VIEWER_IND = 22
BEACON_FILTER_IDX = [
    5, # zero_tracker
    6, # twentry_five
    7, # fifty
    8, # seventry_five
    9, # one_hundred
    #10, # click_tracker
    #11, # customization_id
    #12, # custom_type_id
    #13, # custom_report_id
    #14, # custom_event_id
    #15, # is_media_buy
    #16, # request_time
    22, # viewer_id
    #27, # volume_percent
    30, # state
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

