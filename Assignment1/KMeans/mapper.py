#!/usr/bin/env python
"""mapper.py"""

import sys
from math import sqrt
import numpy as np
import random

class KmeansBuildCluster:
    """mapper function for creating Kmeans cluster"""
    def __init__(self, filepath, k=3):
        """
        filepath : path to file containing list of datapoints
        k        : number of clusters
        n_points : number of points in the datafile 
        """
    
        self.filepath  = filepath
        self.k         = k
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

    def get_centroids(self):
        """initializes centroids from the data points"""

        for _ in range(self.k):
            rand_idx   = random.randint(0, self.n_points-1)
            centroid_x = self.x_list[rand_idx]
            centroid_y = self.y_list[rand_idx]
            self.centroids.append([centroid_x, centroid_y])

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
    datafile = 'data_points.txt'

    kmeans_cluster = KmeansBuildCluster(datafile)
    kmeans_cluster.get_centroids()
    kmeans_cluster.create_clusters()
