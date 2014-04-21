#!/usr/bin/env python

import sys

STATE_IND = 24
COOKIE_IND = 44

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

