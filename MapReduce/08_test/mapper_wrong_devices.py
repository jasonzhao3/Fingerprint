#!/usr/bin/env python
'''
    This map-reduce job takes in beacon record data,
    and output frequency-based profile of each device

    Mapper Output Format:
    identifier  \t  frequenced-based feature list 

    Its corresponding reducer is an identical reducer.
'''

import sys

devices = ['Mp9u25ZAw2M5TN,J8hCe9tQcsmeiM,GoBB98olu5Rtmj,8ZVhMjTNNVCpoe', 'j0eyoRorAHKEOx,QuDAgBTAKs6nqK', '6HKn1NnLdXx6ec,reI5cJInAxS8M4,HmrWJUqSsjAuAv,heXkYGbveNHu0t,kKesXk52cUDWqI,hDV7i4xCj4pgjq,L1mFbzvYGUciEp,E9pWjzqGK7DpYB,1AWDWXByiLycIM,3OeARGuw7YelnL,Ei22E67J0WQ2EN,ZH5XAkwR0HblfA,ShGFKLAjat7MD4,vLKkotuKEdZSD6,8Q8lDizsp7V0Du,meRkLIOdnkLBWQ,NKX7Jmk9oNLL4y,R1iRLI9daKz0CL,Ci74iVNbYEelBS,CunXWBButqhkqq,7U191dUY6Bhqwk,jcrpV9pHMgPLyt,NRllwBMC9V5sC6,2MUmxMag8Hl1Af,ejzX7y1PaMXACS,ydX4B7nGDnBdjZ,re8BcBX8tDlkEv,zYmbd5LK0nX3fh,u2DmgROjARPVkG,lCbiLo7SjpdQWl,mGRIG01uNvyjOG,ZMOFXE55hK1uxT,dKwD1MLr6uNXvq,P5OTttzNSyHzza,ebPlYJgvSJRWpb', 
'elwjVsVJbM0nAH,ZeTZNaLpXHphT1', 'f19s3VSJ2kXWux,cPhQ1URo6iwzNY,5iXch5J2YF2WYG,KkN8fkDhtA5sEe', 'NxqHFd2mfPbopD,xDdpceTcv27mBg,aGHB6fjw97r7DN,jEVvxtmbAXkAZq', 'CvMa7SV3isbU5W,G0zii8fJxWo31G', 'lrx1TkzUozWeio,cvDiQFmIT964hm','2lPx5TXCtIySsU,QN5fE22xRS6PhE,H6HnjRFC5IoPpH','BloZNcioUuedWo,KRhgGyUVWtwBWs,zf7aUJ67qeD4Vl', 'uSgKMPzWn7cbVR,hsp96tzYz5nDmU,aM2diLL1Jza2dW,pcXV8lo4xbK5rm,nUwuk9mer8VGxC,Wah7LdWYxBmJS6,Yjxay0BuIt7Woq,nI1RpQAOHCejPP,rcgNh6mNUPNjmJ,2IDP9OsTQjZz0l,tAxUZUqamQdby0,OBl5XThab5SjX2,jvWDGKkPj3q3xO,151zyC92Pa9MEb,7m8pu4rZKKRBcI,7wDQ5jG4SCq0za,G999iqQ20K97Lm,LxOAVTIerxKDLJ,ecXJ9fBijibVIx,XJCDCa8So5e555',
'CrrUoc47sCQXYq,NcYe9Uv1h4jlQw,exV0azL4i0qYZE,JyxUXvHnrdbxBD,6G41jKd389RPXX', 'odKR05qgn3KRJL,CBoM4yWujRtaqO,Bh5cACzQWbm6Xo,YPhG0aBjmwL7nP,3Oro98AW3PXPRl', 'I9GC7Fw4LE1ZSh,RtexuDHBdRSF3j','wCxjTT8n2Zj1Z0,y6cL4duRonxMwc','mv8sY7FghkTIqm,oYjKyJcCGVKnKi,lslxHzT5WVfHXj,sDbWza5UGe2mcd,K5cPSSRA8Ya8W3,OgeSf8WecheYhv', 'SyajccjaYIW8t2,BRfNjQXeHSVVKX,HRoK4izqIxHm3X,NagjsTr7uA3r2z,knGOfWHqSm7zv8','jcd61TFZGSmnLj,YCmuEPxjKGLAwC,KBHZYuUunTNWiV,RPmswVRzSk1uJn,nzGOtdVCugq1B7,NI9eCUExLjLGQG,3qjlHdwKShZieO']
res = ['']*len(devices)
# input comes from STDIN (standard input)
for line in sys.stdin:
      isBreak = False;
      line = line.strip()
      identifier, value = line.split('\t')
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