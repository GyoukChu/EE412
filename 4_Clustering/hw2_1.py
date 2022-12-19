'''
    Assignment #2
    hw2_1.py

    Created by 20200678 GyoukChu on 2022/11/01
    Last Updated on 2022/11/02
'''
# Note: Only import followings;
# pyspark, numpy, re, sys, math, csv, time, and os
import sys
from pyspark import SparkConf, SparkContext

def mysplit(lines):
    '''
        Split the given string as decribed below.

        Input : list of strings,
        each string(line) : <FEATURE 1> <FEATURE 2> ... <FEATURE 58>
    
        Output : list of points,
        each point is a list of FEATURES, which is float.
    '''
    result = []
    for line in lines:
        point = line.split(" ")
        for i in range(len(point)):
            point[i] = float(point[i])
        result.append(point)
    return result

def distance(x, y):
    '''
        Calculate the Euclidean distance btw two vectors

        Input : two vectors, x, y
        Output : Euclidean distance btw x and y
    '''
    assert len(x) == len(y)

    dist = 0.0
    for i in range(len(x)):
        dist += (x[i]-y[i]) ** 2

    dist = pow(dist, 0.5)
    return dist   

def initialize_clusters(points, k):
    '''
        !! Pick the first point in the dataset, not randomly.
        WHILE there are fewer than k points DO
            Add the point whose minimum distance from the 
            selected points is as large as possible;
        END:

        Input : points, k
        Output : list of k centroid points, 
                points except centroids
    '''
    centroids = []

    centroids.append(points.pop(0))

    while len(centroids) < k:
        max_index = 0
        max_dist = 0
        for index, point in enumerate(points):
            # minimum distance btw point and selected points
            min_dist = float("inf") 
            for centroid in centroids:
                d = distance(point, centroid)
                if d < min_dist:
                    min_dist = d
            # if minimum distance btw point and centroids
            # > max_dist, then update
            if min_dist > max_dist:
                max_index = index
                max_dist = min_dist
        centroids.append(points.pop(max_index))

    return centroids, points


class Cluster:
    def __init__(self, centroid):
        self.points = []
        self.centroid = centroid
        self.points.append(centroid)
    
    def add(self, point):
        self.points.append(point)
        return self
    
    def diameter(self):
        R = 0.0
        if len(self.points) > 1:
            for i in range(len(self.points)):
                for j in range(i+1,len(self.points)):
                    R = max(R, distance(self.points[i], self.points[j]))
        return R

# main
k = int(sys.argv[2])

with open(sys.argv[1], 'r') as file:
    lines = file.readlines()

points = mysplit(lines)
'''
Step 1. W/O Spark
Initially choose k points
Make these points the centroid of their clusters
'''
centroids, points = initialize_clusters(points, k)
# Create k-many clusters
clusters = []

for i in range(k):
    clusters.append(Cluster(centroids[i]))

'''
Step 2. W/ Spark
For each remaining point p DO
    Find the centroid to which p is closest;
    Add p to the cluster of that centroid;
End;
'''

conf = SparkConf()
sc = SparkContext(conf=conf)

RDD_points = sc.parallelize(points) \
    .flatMap(lambda p: ((tuple(p),(i, distance(p, centroids[i]))) for i in range(len(centroids)))) \
    .reduceByKey(lambda x,y:x if x[1]<y[1] else y) \
    .map(lambda p: (p[1][0],p[0]))

for (index, point) in RDD_points.collect():
    clusters[index].add(list(point))

# Calculate avg. diameter of k clusters
sum = 0.0
for cluster in clusters:
    sum += cluster.diameter()
print("For k=%d, the average diameter of k clusters is %.8f." % (k, sum/k))    

# Run : bin\spark-submit hw2_1.py kmeans.txt 3