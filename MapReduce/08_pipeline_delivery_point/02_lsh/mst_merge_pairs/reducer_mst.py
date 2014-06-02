#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter
from sets import Set
"""
MST Spanning Tree Finding WCT
"""
clusters = []
indMap = {}
def mergeCluster(start, end):
    startInd = -1
    #print startInd
    if (start in indMap):
        startInd = indMap[start]
   
    finishInd = -1
    if (end in indMap):
        finishInd = indMap[end]
    
    if finishInd == startInd and startInd == -1:
        # no index found   
        newSet = Set()
        newSet.add(start)
        newSet.add(end)
        clusters.append(newSet)
        indMap[start] = len(clusters) - 1
        indMap[end] = len(clusters) - 1
        
    elif finishInd != startInd:
        if finishInd == -1:
            clusters[startInd].add(end)
            indMap[end] = startInd
        elif startInd == -1:
            clusters[finishInd].add(start)
            indMap[start] = finishInd
        else:
            # merging
            for elem in clusters[finishInd]:
                clusters[startInd].add(elem)
                indMap[elem] = startInd
            clusters[finishInd] = None



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
            start = edge[0].strip().strip('\'')
            end = edge[1].strip().strip('\'')
            mergeCluster(start, end)
    
      except (RuntimeError, TypeError, NameError, ValueError, IOError):
            # count was not a number, so silently discard this item
        print "ERROR!!"
        pass
    
    for i in range(len(clusters)):
        #print ",".join(clusters[i])
        if clusters[i]:
            print ("%d%s%s" %(i, '\t', ",".join(clusters[i])))

if __name__ == "__main__":
    main()