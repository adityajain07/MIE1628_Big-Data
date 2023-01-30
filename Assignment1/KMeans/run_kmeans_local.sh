#!/bin/bash

TIMEFORMAT='It took %R seconds to execute the script.'
time {

i=1

while :
do  
    if [ $i = 1 ]
	then
        python mapper.py --initialize random --datafile data_points.txt --num_clusters 6 | sort | python reducer.py    
	else
		python mapper.py --initialize fromfile --datafile data_points.txt --num_clusters 6 | sort | python reducer.py
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