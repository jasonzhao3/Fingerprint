# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 14:32:20 2014

@author: zhulk
"""
import sys, os, csv
path = '/Users/zhulk/Downloads/'
data_file_name = 'part-00002-8'
data_file = os.path.join (path, data_file_name)

ins = open( data_file, "r" )
count = 0
for line in ins:
    (key, value) = line.split('\t')
    valueList = value.split(',')
    count += len(valueList)

print count