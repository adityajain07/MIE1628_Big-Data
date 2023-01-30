"""
About: Testing script for the KMeans questions in MIE1628 A1
Date : January 27, 2023
"""
import matplotlib.pyplot as plt

datafile = 'data_points.txt'

def load_points(filepath):
    """loads and returns the points from the text file"""

    f      = open(filepath, 'r')
    x_list = []
    y_list = []

    for line in f:
        line = line.strip()
        x, y = line.split(',')
        x_list.append(round(float(x), 2))
        y_list.append(round(float(y), 2))
    f.close()

    return x_list, y_list

x_array, y_array = load_points(datafile)

plt.scatter(x_array, y_array)
plt.show()
