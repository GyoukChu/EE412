'''
    Assignment #3
    hw3_1.py

    Created by 20200678 GyoukChu on 2022/11/15
    Last Updated on 2022/11/15
'''
# Note: Only import followings;
# pyspark, re, sys, math, csv, time, collections,
# itertools, and os

import sys
from pyspark import SparkConf, SparkContext
# import time
# start = time.time()

def mysplit(line):
    '''
        Split the given string as decribed below.
        
        Input:<SourceID> <Dest.ID> (between them, there is whitespace char.)
        Output:(Dest.ID, SourceID)
    '''
    source, dest = line.split()
    source = int(source)
    dest = int(dest)
    return (dest, source)

def my_mat_mul_vec(M, V):
    '''
        Do multiplication between matrix and vector.

        Input: M(pyspark RDD): ((i,j),m)
               V(pyspark RDD): (j,v)
        Output: MV(pyspark RDD): (i,m*v for all j)
    '''
    # ((i,j),m) -> (j,(i,m)) and then join.
    # (j,((i,m),v)) -> (i,m*v) and then reduceByKey by adding.
    MV = M.map(lambda x: (x[0][1], (x[0][0], x[1]))) \
            .join(V).map(lambda x: (x[1][0][0], x[1][0][1]*x[1][1])) \
            .reduceByKey(lambda x, y: x+y)
    
    return MV

# main
beta = 0.9
num_iter = 50
checkpoint = 4
N = 10

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])
pairs = lines.map(mysplit).distinct()

# Initialization
# M consists of ((i,j),m) / from j to i
# - i,j component of transition matrix M is m

M = pairs.map(lambda x: ((x[0], x[1]), 1.0))
# divider of each column, consists of (start node, # of edges)
num_nodes = M.map(lambda x: (x[0][1], x[1])).reduceByKey(lambda x, y: x+y)

# ((i,j),m) -> (j, (i,m)) and then join.
# (j, ((i,m),# of edges)) -> ((i,j), m/# of edges)
M = M.map(lambda x: (x[0][1], (x[0][0], x[1]))).join(num_nodes) \
        .map(lambda x: ((x[1][0][0],x[0]),x[1][0][1]/x[1][1]))

# V consists of (i,v)
# - i component of vector V is v
# E consists of 'all 1/n' vector
temp = pairs.collect()
nodes = []
for t in temp:
    for x in t:
        nodes.append(x)
nodes = set(nodes)
init = []
for n in nodes:
    init.append((n,1/len(nodes)))
V = sc.parallelize(init)
E = sc.parallelize(init)

# Iteration
for i in range(num_iter):
    MV = my_mat_mul_vec(M,V)
    beta_MV = MV.map(lambda x: (x[0],beta*x[1]))
    taxation = E.map(lambda x: (x[0],(1-beta)*x[1]))
    V = beta_MV.union(taxation).reduceByKey(lambda x, y: x+y)
    if (i%checkpoint==0):
        V = V.collect()
        V = sc.parallelize(V)

result = V.collect()
result = sorted(result, key = lambda x : (x[1],-x[0]), reverse=True)

for i in range(min(len(nodes),N)):
    print("%d\t%.5f" % (result[i][0], result[i][1]))

# Run: bin/spark-submit hw3_1.py graph.txt
# end = time.time()
# print("Elapsed time is="+str(end-start))