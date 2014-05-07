#!/usr/bin/env python
import sys

def main(separator='\t'):
	old = ""
    for line in sys.stdin:
    	line.strip()
    	attr_list = line.split(separator)
    	if (attr_list[0] == old):
    		old += line
    		
        print "%s"  % (line)
        
if __name__ == "__main__":
    main()
        

