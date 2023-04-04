#!/usr/bin/env python
"""reducer.py"""

import sys

def calculateNewCentroids():
    '''
    Reducer function:
    Similar to combiner except it is for all machines
    load in what combiner gives
    Combines member, member centroid for each cluster from different machines
    together.
    Update Centroid for each cluster
    output exactly the same format of data as input file except centroid, cluster
    ID have been updated
    '''
    f = open('centroids_current.txt', 'w')

    current_centroid = None
    sum_x = 0
    sum_y = 0
    count = 0
 
    # input comes from STDIN
    for line in sys.stdin:

        # parse the input of mapper.py
        line = line.strip()  
        cluster_id, x, y = line.split('\t') 
       
        # convert count (currently a string) to float, int  
        try:  
            x = float(x)
            y = float(y)  
        except ValueError:  
            # count was not a number, so silently  
            # ignore/discard this line  
            continue 
         

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_centroid == cluster_id:
            count += 1
            sum_x += x
            sum_y += y
        else:
            if current_centroid and count!=0:
                # print the average of every cluster to get new centroids
                output = "%s\t%s\t%s" % (current_centroid, sum_x/count, sum_y/count)
                f.write(output)
                f.write('\n')
                print(output)

            current_centroid = cluster_id
            sum_x = x
            sum_y = y
            count = 1
    
    # print last cluster's centroids
    if current_centroid == cluster_id and count != 0:
        output = "%s\t%s\t%s" % (current_centroid, sum_x/count, sum_y/count)
        f.write(output)
        print(output)

    f.close()

if __name__ == "__main__":
    calculateNewCentroids()