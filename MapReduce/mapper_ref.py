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
    words = line.split()
    # increase counters
    for word in words:
        lword = word.lower()
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        if pattern.match(lword):
            print '%s%s%d' % (lword, "\t", 1)