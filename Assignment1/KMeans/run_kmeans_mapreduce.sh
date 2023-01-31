#!/bin/bash

TIMEFORMAT='It took %R seconds to execute the script.'
time {

i=1

while :
do  
    if [ $i = 1 ]
	then
        hadoop jar /home/hdoop/hadoop-3.2.4/share/hadoop/tools/lib/hadoop-streaming-3.2.4.jar -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/mapper.py -mapper 'python mapper.py --initialize random --datafile data_points.txt --num_clusters 6' -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/reducer.py -reducer 'python3 reducer.py' -input inputword -output outp    
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