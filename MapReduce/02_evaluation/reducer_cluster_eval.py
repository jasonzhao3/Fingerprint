#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys
 
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # print data
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
    for key, group in groupby(data, itemgetter(0)):
        try:
            error_cnt = sum(int(counts.split(',')[0]) for key, counts in group)
            total_cnt = sum(int(counts.split(',')[1]) for key, counts in group)
            print "%s%s%d%s%d" % (key, separator, error_cnt, ',', total_cnt)
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()

    