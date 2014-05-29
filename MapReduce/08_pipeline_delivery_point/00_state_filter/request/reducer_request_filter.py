#!/usr/bin/env python

'''
    Reducer Output Format: 
    identifier_deliverypoint \t record1 | record2 | record3 ... 
'''
 
from itertools import groupby
from operator import itemgetter
import sys
 
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)

    for identifier, group in groupby(data, itemgetter(0)):
        try:
            record = "|".join(v for identifier, v in group)
            print "%s%s%s" % (identifier, separator, record)
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()
