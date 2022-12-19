'''
    Assignment #4
    hw4_2_p3.py

    Created by 20200678 GyoukChu on 2022/12/10
    Last Updated on 2022/12/17
'''
# Note: Only import followings;
# numpy, sys, math, csv, os

import sys
import numpy as np

def mysplit(lines):
    window = []
    for line in lines:
        window.append(int(line))
    return window

class bucket:
    def __init__(self, timestamp, size):
        self.timestamp = timestamp
        self.size = size # Actual # of ones is 2 ^ size

    def merge(self, another_bucket):
        self.timestamp = max(self.timestamp, another_bucket.timestamp)
        self.size += 1
        return self

class DGIM:
    def __init__(self):
        self.buckets = [[]]
    
    def add(self, bucket):
        self.buckets[0].append(bucket)
        # Merge buckets as many as possible
        for i in range(len(self.buckets)):
            if len(self.buckets[i]) > 2: # 3 buckets of same size, 2^i
                bucket1 = self.buckets[i].pop(0)
                bucket2 = self.buckets[i].pop(0)
                if (i+1) < len(self.buckets):
                    self.buckets[i+1].append(bucket1.merge(bucket2))
                else:
                    self.buckets.append([])
                    self.buckets[i+1].append(bucket1.merge(bucket2))

    def estimate(self, min_timestamp):
        result = 0
        for pair in self.buckets:
            for bucket in pair:
                if bucket.timestamp < min_timestamp:
                    continue
                else:
                    result += 2**bucket.size
                    max_count = 2**bucket.size
        result -= max_count/2.0
        return result

# main
with open(sys.argv[1], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))

window = mysplit(lines)
queries = sys.argv[2:]
for i in range(len(queries)):
    queries[i] = int(queries[i])

my_DGIM = DGIM()
''' Divide window into buckets, named DGIM '''
for timestamp, bit in enumerate(window):
    if bit:
        my_DGIM.add(bucket(timestamp, 0))

''' Answer for each query '''
for k in queries:
    result = my_DGIM.estimate(len(window)-k)
    print(result)

# Run : python3 hw4_2_p3.py stream.txt 1 10 100