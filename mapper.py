#!/usr/bin/env python

import sys
import re
import string

pattern = re.compile("^[a-z][a-z0-9]*$")

# input comes from STDIN (standard input)

lines = ['0A8018650145345589B7AB4252B85E9B,6597,248944,118253,0,1,0,0,0,0,0,0,0,0,0,0,2014-04-06 00:00:00.0,0,0,0,1E-8,,72_198_197_86_9vHxq6V0P3bQnd,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,NA,NA,NA,0,0,0,0','0A80186D01453455387E72BF6EC3FF29,0,266557,124718,0,0,0,0,1,0,0,0,0,0,0,0,2014-04-06 00:00:00.009,0,0,0,0.0,,69_13_26_190_ACInXWO3WckQ5o,0,0,0,0,100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,NA,NA,NA,0,0,0,0','0A8018920145345572BA4C9F14FC4812,0,270175,124183,0,0,1,0,0,0,0,0,0,0,0,0,2014-04-06 00:00:00.012,0,0,0,0.0,,63.152.98.1,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,NA,NA,NA,0,0,0,0']
for line in lines:
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