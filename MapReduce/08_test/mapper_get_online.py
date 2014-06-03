#!/usr/bin/env python
'''

'''

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
      line = line.strip()
      identifier, value = line.split('\t')
      sessions = 
      for i in range(0, len(devices)):
        if isBreak: 
          break
        dStr = devices[i]
        for j in range(0, len(dStr.split(','))):
          target = dStr.split(',')[j]
          if identifier == target:
              res[i] += '##################################' + line
              isBreak = True
              break
for i in range(0, len(res)):
  print '%s%s%s' % (i, "\t", res[i])