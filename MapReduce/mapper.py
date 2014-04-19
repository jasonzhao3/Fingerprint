#!/usr/bin/env python

import sys
import re
import string

pattern = re.compile("^[a-z][a-z0-9]*$")

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    attr_list = line.split(',')
    cookie = attr_list[22]
    fields = cookie.split('_')
    if (len(fields) == 5):
        identifier = fields[4]
        attr_list[22] = fields[0] + "." + fields[1] + "." + fields[2] + "." + fields[3]
    elif (len(fields) == 1):
        identifier = "null"
#    attr_num = len(attr_list)
#    toPrint = '%s%s'
#    cnt = 0
#    toPrintArgs = ''
#    for i in range(0, attr_num)
#        toPrint += '%s'
#        cnt++
#        if (cnt == 22)
#            cnt++
#        if i == attr_num - 1
#            toPrintArgs += 'str[' + str(i) + ']b'
#        else
#            toPrintArgs += 'str[' + str(i) + '],'
    print '%s%s%s' % (identifier, "\t", ', '.join(attr_list))