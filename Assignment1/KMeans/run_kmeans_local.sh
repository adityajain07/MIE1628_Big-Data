#!/bin/bash

TIMEFORMAT='It took %R seconds to execute the script.'
time {

i=1

while :
do  
    if [ $i = 1 ]
	then
        cat data_points.txt | python mapper.py --initialize random --num_clusters 3 | sort | python reducer.py    
	else
		cat data_points.txt | python mapper.py --initialize fromfile --num_clusters 3 | sort | python reducer.py
	fi
    convergeflag=$(python check_convergence.py)
    echo "Done with iteration" $i    

    if [ $convergeflag = 1 ]
	then
        echo "Converged at iteration" $i
        cp centroids_current.txt centroids_final.txt
		break
	else
		cp centroids_current.txt centroids_previous.txt
        rm centroids_current.txt
	fi
	i=$((i+1))

    # break

done
}