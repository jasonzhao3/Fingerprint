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
FILTER_IDX = [1, 2, 3, 4, 6, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 43, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
60, 61, 62, 63, 64, 65, 66, 67, 68, 69]
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

