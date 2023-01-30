#!/usr/bin/env python
"""mapper.py"""

import numpy as np
import random
import argparse

class KmeansBuildCluster:
    """mapper function for creating Kmeans cluster"""
    def __init__(self, args):
        """
        filepath : path to file containing list of datapoints
        k        : number of clusters
        n_points : number of points in the datafile 
        """
    
        self.filepath  = args.datafile
        self.k         = args.num_clusters
        self.n_points  = None
        self.x_list    = None
        self.y_list    = None
        self.centroids = []

        self._read_data()

    def _read_data(self):
        """reads data points from the text file"""

        f      = open(self.filepath, 'r')
        x_list = []
        y_list = []

        for line in f:
            line = line.strip()
            x, y = line.split(',')
            x_list.append(round(float(x), 2))
            y_list.append(round(float(y), 2))
        f.close()

        self.n_points = len(x_list)
        self.x_list   = x_list
        self.y_list   = y_list

    def get_random_centroids(self):
        """initializes centroids from the data points"""
        
        file = open('centroids_previous.txt', 'w')
        for i in range(self.k):
            rand_idx   = random.randint(0, self.n_points-1)
            centroid_x = self.x_list[rand_idx]
            centroid_y = self.y_list[rand_idx]
            self.centroids.append([centroid_x, centroid_y])

            output = "%s\t%s\t%s" % (i, centroid_x, centroid_y)
            file.write(output)
            file.write('\n')
        file.close()

    def get_centroids_from_file(self):
        """reads the centroids from the file"""

        f      = open('centroids_previous.txt', 'r')
        for line in f:
            line = line.strip()
            _, centroid_x, centroid_y = line.split('\t')
            self.centroids.append([float(centroid_x), float(centroid_y)])
        f.close()

    def create_clusters(self):
        """creates clusters based on current centroids"""

        for i in range(self.n_points-1):
            min_dist   = 1e8
            cluster_id = None
            x, y       = self.x_list[i], self.y_list[i]

            for centroid_idx in range(self.k):
                centroid  = np.array(self.centroids[centroid_idx])
                datapoint = np.array((x, y))
                dist      = np.linalg.norm(centroid - datapoint)

                if dist < min_dist:
                    min_dist = dist
                    cluster_id = centroid_idx

            print("%s\t%s\t%s" % (cluster_id, x, y))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--initialize', help = 'whether to initalize clusters randomly from the data points', required=True)
    parser.add_argument('--datafile', help = 'path to the file containing data points', required=True)
    parser.add_argument('--num_clusters', help = 'number of clusters', type=int, required=True)
    args   = parser.parse_args()

    kmeans_cluster = KmeansBuildCluster(args)
    if args.initialize=='random':
        kmeans_cluster.get_random_centroids()
    else:
        kmeans_cluster.get_centroids_from_file()

    kmeans_cluster.create_clusters()
