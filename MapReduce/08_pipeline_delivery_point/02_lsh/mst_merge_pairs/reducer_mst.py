#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
from sets import Set
"""
MST Spanning Tree Finding WCT
"""
clusters = []
def mergeCluster(start, end):
    startInd = -1
    #print startInd
    for i in range(0, len(clusters)):
        if start in clusters[i]:
            startInd = i
   
    finishInd = -1
    for i in range(0, len(clusters)):
        if end in clusters[i]:
            finishInd = i
    
    if finishInd == startInd and startInd == -1:
        # no index found
        
        newSet = Set()
        newSet.add(start)
        newSet.add(end)
        clusters.append(newSet)
        
    if finishInd != startInd:
        if finishInd == -1:
            clusters[startInd].add(end)
        elif startInd == -1:
            clusters[finishInd].add(start)
        else:
            # merging
            for elem in clusters[finishInd]:
                clusters[startInd].add(elem)
            clusters.pop(finishInd)



def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
        
# concat version
def main(separator='\t'):
  # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
   
    # one group corresponds to one bucket
    for key, group in groupby(data, itemgetter(0)):   
      try:
        # get each device and merge
        for key, edgeStr in group:
            #print key
            #print edgeStr
            edge = edgeStr.split(',')
            start = edge[0]
            end = edge[1]
            mergeCluster(start, end)
    
      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
    
    for i in range(len(clusters)):
        #print ",".join(clusters[i])
        print ("%d%s%s" %(i, '\t', ",".join(clusters[i])))

if __name__ == "__main__":
    main()