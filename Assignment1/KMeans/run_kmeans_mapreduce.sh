#!/bin/bash

TIMEFORMAT='It took %R seconds to execute the script.'
time {

hdfs dfs -mkdir -p inputData
hdfs dfs -put /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/data_points.txt inputData
i=1

while :
do  
    if [ $i = 1 ]
	then
        hadoop jar /Users/adityajain/hadoop/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/mapper.py -mapper 'python mapper.py --initialize random --num_clusters 3' -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/reducer.py -reducer 'python reducer.py' -input inputData -output outCentroids    
	else
		hadoop jar /Users/adityajain/hadoop/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/mapper.py -mapper 'python mapper.py --initialize fromfile --num_clusters 3' -file /Users/adityajain/Dropbox/UofT_Studies/MIE1628/Assignment1/KMeans/reducer.py -reducer 'python reducer.py' -input inputData -output outCentroids
	fi

	break

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