#!/usr/bin/env python  
  
from operator import itemgetter  
import sys  
  
current_line = None  
current_count = 0  
line = None  
  
# input comes from STDIN  
for line in sys.stdin:  
    # remove leading and trailing whitespace  
    line = line.strip()  
  
    # parse the input we got from mapper.py  
    line, count = line.split('\t', 1)  
  
    # convert count (currently a string) to int  
    try:  
        count = int(count)  
    except ValueError:  
        # count was not a number, so silently  
        # ignore/discard this line  
        continue  
  
    # this IF-switch only works because Hadoop sorts map output  
    # by key (here: line) before it is passed to the reducer  
    if current_line == line:  
        current_count += count  
    else:  
        if current_line:  
            # write result to STDOUT  
            print ('%s\t%s' % (current_line, current_count) )
        current_count = count  
        current_line = line  
  
# do not forget to output the last line if needed!  
if current_line == line:  
    print ('%s\t%s' % (current_line, current_count)) 