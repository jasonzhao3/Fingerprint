#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys
 
'''
  This is a script for pair-pair result evaluation
  For different similarity threshold, we evaluate the error rate
  based on geographical distance.
'''

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    print data
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
    for current_threshold, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_threshold, count in group)
            print "%s%s%d" % (current_threshold, separator, total_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()

    