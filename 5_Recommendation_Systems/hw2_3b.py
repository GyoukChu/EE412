'''
    Assignment #2
    hw2_3b.py

    Created by 20200678 GyoukChu on 2022/11/01
    Last Updated on 2022/11/02
'''
# Note: Only import followings;
# pyspark, numpy, re, sys, math, csv, time, and os
import sys
import math
import numpy as np

def mysplit(lines):
    '''
        Split the given string as decribed below.

        Input : list of strings,
        each string(line) : <USER ID>,<MOVIE ID>,<RATING>,<TIMESTAMP>
    
        Output : list of lists, [<USER ID>,<MOVIE ID>,<RATING>,<TIMESTAMP>]
            all of them are changed to int except RATING 
            (RATING is float because of .5 in RATING)
    '''
    result = []
    for line in lines:
        USERID,MOVIEID,RATING,TIMESTAMP = line.split(",")
        USERID = int(USERID)
        MOVIEID = int(MOVIEID)
        RATING = float(RATING)
        TIMESTAMP = int(TIMESTAMP)
        result.append([USERID,MOVIEID,RATING,TIMESTAMP])
    return result

def distance(x, y):
    '''
        Calculate the Cosine distance btw two vectors

        Input : two vectors, x, y
        each vector is list of (position, value)

        Output : Cosine distance btw x and y
    '''
    norm_x = 0.0
    norm_y = 0.0
    dot_xy = 0.0
    for x_pos, x_value in x:
        norm_x += x_value ** 2
        for y_pos, y_value in y:
            if x_pos == y_pos:
                dot_xy += x_value * y_value

    for y_pos, y_value in y:
        norm_y += y_value ** 2
    norm_x = pow(norm_x, 0.5)
    norm_y = pow(norm_y, 0.5)

    if (norm_x==0) or (norm_y==0):
        return float("inf") # Not worth considering if zero vector
    
    # There exists a issue on significant figures in calculation, so just round off. 
    dist = math.acos(round(dot_xy/(norm_x * norm_y), 8))
    return dist 

def make_utility_matrix(data):
    '''
        Make a utility_matrix. (its type is dict.)

        Input : list of lists,
            each list : <USER ID>,<MOVIE ID>,<RATING>,<TIMESTAMP>

        Output : Dictionary,
            key : (<USER ID>,<MOVIE ID>) - analogous to matrix index i,j
            value : <RATING>
    '''      
    M = dict()
    for USERID,MOVIEID,RATING,_ in data:
        M[(USERID,MOVIEID)] = RATING
    return M

def normalize_utility_matrix(utility_matrix):
    '''
        Make a utility_matrix normalized.

        Input : Dictionary,
            key : (<USER ID>,<MOVIE ID>) - analogous to matrix index i,j
            value : <RATING>

        Output : Same dictionary, but value is normalized.
    '''
    M_normalized = utility_matrix.copy()
    temp = {} # key is <USER ID>, value is [<RATING1>, <RATING2>, ...]
    keys = list(utility_matrix.keys())
    for key in keys:
        USERID, MOVIEID = key
        if USERID in temp:
            temp[USERID].append(utility_matrix[key])
        else:
            temp[USERID] = [utility_matrix[key]]

    for key in temp:
        temp[key] = sum(temp[key])/len(temp[key]) # key is <USER ID>, value is avg. RATINGs

    for key in keys:
        USERID, MOVIEID = key
        M_normalized[key] -= temp[USERID]

    return M_normalized

def find_similarity(M_normalized, target_user, target_item, top_n, mode):
    '''
        Find top-n similar_object (list) depend on mode

        Input : M_normalized, target_user, target_item, mode
                mode : "user" - top-n user similar to target_user
                       "item" - top-n item similar to target_item

        Output : list of similar_objects
    '''

    keys = list(M_normalized.keys())

    similar_objects = []
    if mode == "user":
        users = {} # key is <USER ID>, value is list of (<MOVIE ID>, <RATING(normalized)>)
        for key in keys:
            USERID, MOVIEID = key
            if USERID in users:
                users[USERID].append((MOVIEID,M_normalized[key]))
            else:
                users[USERID]= [(MOVIEID,M_normalized[key])]

        if target_user not in users:
            return []

        for user in users:
            if user==target_user: continue
            else:
                similar_objects.append((user,distance(users[user], users[target_user])))
        # Ascending order in distance, and then ascending order in <USER ID>
        similar_objects.sort(key = lambda x : (x[1],x[0]))
        result = [similar_objects[i][0] for i in range(top_n)]      
        return result
    
    elif mode == "item":
        items = {} # key is <MOVIE ID>, value is list of (<USER ID>, <RATING(normalized)>)
        for key in keys:
            USERID, MOVIEID = key
            if MOVIEID in items:
                items[MOVIEID].append((USERID,M_normalized[key]))
            else:
                items[MOVIEID] = [(USERID,M_normalized[key])]
        
        if target_item not in items:
            return []
        
        for item in items:
            if item in range(1,1001): continue # except ID=1 to ID=1000
            else:
                similar_objects.append((item,distance(items[item], items[target_item])))
        # Ascending order in distance, and then ascending order in <USER ID>
        similar_objects.sort(key = lambda x : (x[1],x[0]))      
        result = [similar_objects[i][0] for i in range(top_n)]    
        return result
    
    else:
        assert(False)
    

def collaborative_filtering(utility_matrix, target_user, target_item, mode):
    '''
        Do collaborative_filtering and estimate rating of target_user to tatget_item

        Input : utility_matrix, target_user, target_item,
                mode : "user" - user-based approach
                       "item" - item-based approach

        Output : (target_item, predicted_rating)
    '''
    M_normalized = normalize_utility_matrix(utility_matrix)
    
    if mode == "user":
        similar_users = find_similarity(M_normalized, target_user, target_item, top_n=10, mode=mode)
        predicted_rating = 0
        cnt = 0
        for similar_user in similar_users:
            if (similar_user, target_item) in utility_matrix:
                predicted_rating += utility_matrix[(similar_user, target_item)]
                cnt +=1
        if cnt!=0:
            predicted_rating /= cnt
        return (target_item, predicted_rating)

    elif mode == "item":
        similar_items = find_similarity(M_normalized, target_user, target_item, top_n=10, mode=mode)
        predicted_rating = 0
        cnt = 0
        for similar_item in similar_items:
            if (target_user, similar_item) in utility_matrix:
                predicted_rating += utility_matrix[(target_user, similar_item)]
                cnt +=1
        if cnt!=0:
            predicted_rating /= cnt
        return (target_item, predicted_rating)
    
    else:
        assert(False)

# main

target_user = 600
target_items = range(1,1001)

with open(sys.argv[1], 'r') as file:
    lines = file.readlines()

data = mysplit(lines)

M = make_utility_matrix(data)

result_user_based = []
result_item_based = []

top_r = 5

for ID in target_items:
    result_user_based.append(collaborative_filtering(M, target_user, target_item=ID, mode='user'))

result_user_based.sort(key = lambda x : (x[1],-x[0]), reverse=True)

for i in range(top_r):
    print(str(result_user_based[i][0])+"\t"+str(result_user_based[i][1]))

for ID in target_items: 
    result_item_based.append(collaborative_filtering(M, target_user, target_item=ID, mode='item'))

result_item_based.sort(key = lambda x : (x[1],-x[0]), reverse=True)

for i in range(top_r):
    print(str(result_item_based[i][0])+"\t"+str(result_item_based[i][1]))

# Run : python hw2_3b.py ratings.txt
