#!/usr/bin/env python
import sys

def main(separator='\t'):
    for line in sys.stdin:
        print "%s"  % (line)
        
if __name__ == "__main__":
    main()
        

