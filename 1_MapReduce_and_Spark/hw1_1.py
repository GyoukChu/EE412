'''
    Assignment #1
    hw1_1.py

    Created by 20200678 GyoukChu on 2022/10/01
    Last Updated on 2022/10/05
'''
# Note: Only import followings;
# pyspark, numpy, re, sys, math, csv, and os
import sys
from pyspark import SparkConf, SparkContext

# import time
# start = time.time()

def mysplit(line):
    '''
        Split the given string as decribed below.
        
        Input:UserID   FriendID1,FriendID2,FriendID3,...
        Output:((UserID, FriendIDi), 0) and ((FriendIDi, FriendIDj), 1)
    '''
    pairs = []
    
    line = line.split('\t')
    user = line[0]
    
    if line[1] != '': # Have friend list
        friends = line[1].split(",")
        for i in range(len(friends)):
            # 1 - ((UserID, FriendIDi), 0) - they are friend
            key = (min(user,friends[i]),max(user,friends[i]))
            pairs.append((key,0))
            for j in range(i+1,len(friends)):
                # 2 - ((FriendIDi, FriendIDj), 1) - they are not friend (yet)
                key = (min(friends[i],friends[j]),
                        max(friends[i],friends[j]))
                pairs.append((key,1))
    return pairs
'''
    1. Map Task
    Initially assume there are no friends at all.

    Input :UserID   FriendID1,FriendID2,FriendID3,...

    Output : key-value pairs
    Key : (User ID1, User ID2) where ID1 < ID2
    Value : 
    0 if one user and the other user on the friend list are keys
    1 if two people in one user's friend list are keys

    Example;
    Input :0   1,2,3
    Output :[((0,1), 0), ((0,2), 0), ((0,3), 0), ((1,2), 1), ((1,3), 1), ((2,3), 1)]
    
'''

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

pairs = lines.flatMap(mysplit)

'''
    2. Reduce Task
    Checks whether a pair with a value of 1 is really friend or not
    & Count how many pairs of the same key with a value of 1 if not really friends
    
    Input : key-value pairs - ((User ID1, User ID2), 0 or 1)

    Output : ID1    ID2    (# of mutual friends)

    Example; 
    Input :[((0,1), 0), ((0,2), 0), ((0,3), 0), ((1,2), 1), ((1,3), 1), ((2,3), 1),
            ((1,0), 0), ((1,3), 0), ((0,3), 1), 
            ((0,2), 0), 
            ((0,3), 0), ((1,3), 0), ((0,1), 1)]
    Output :1   2   1
            2   3   1
    
'''
pairs = pairs.groupByKey().filter(lambda p:0 not in p[1]) 
# filter out who are already friend each other

counts = pairs.map(lambda p: (p[0],sum(p[1]))) # Count # of mutual friends

results = counts.collect()
results = sorted(results, key=lambda x:(-x[1], x[0][0], x[0][1])) 
# sorting result in a given way

# print the results
temp = 0
for ((id1, id2), count) in results:
    if temp >= 10:
        break
    else:
        print(id1+'\t'+id2+'\t'+str(count))
        temp += 1

# end = time.time()
# print("Elapsed time is="+str(end-start))