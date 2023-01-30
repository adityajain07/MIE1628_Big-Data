#!/usr/bin/env python

import numpy as np

def check_convergence():
    """function to test if the clustering has converged"""

    # reading current centroids 
    curr_centroids_file = 'centroids_current.txt'
    f_curr      = open(curr_centroids_file, 'r')
    x_list_curr = []
    y_list_curr = []
    for line in f_curr:
        line = line.strip()
        _, x, y = line.split('\t')
        x_list_curr.append(round(float(x), 2))
        y_list_curr.append(round(float(y), 2))
    f_curr.close()

    # reading previous centroids 
    prev_centroids_file = 'centroids_previous.txt'
    f_prev      = open(prev_centroids_file, 'r')
    x_list_prev = []
    y_list_prev = []
    for line in f_prev:
        line = line.strip()
        _, x, y = line.split('\t')
        x_list_prev.append(round(float(x), 2))
        y_list_prev.append(round(float(y), 2))
    f_prev.close()

    for i in range(len(x_list_curr)):
        centroid_curr = np.array((x_list_curr[i], y_list_curr[i]))
        centroid_prev = np.array((x_list_prev[i], y_list_prev[i]))
        distance      = np.linalg.norm(centroid_curr - centroid_prev) 
        if distance > 1:
            print(0)  # not converged
            return 

    print(1)  # converged

if __name__ == "__main__":    
    check_convergence()
