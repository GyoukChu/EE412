'''
    Assignment #1
    hw1_2.py

    Created by 20200678 GyoukChu on 2022/10/03
    Last Updated on 2022/10/05
'''
# Note: Only import followings;
# pyspark, numpy, re, sys, math, csv, and os
import sys

# import time
# start = time.time()

f = open(sys.argv[1],"r") # file read
baskets = f.readlines()

def getindex(i,j,new_index):
    '''
        Return the index of one-dimensional triangular array
        With reference to page 223 of the textbook, 
        some modifications were made. (starts w/ 0, etc.)
    '''
    return int(i*(new_index-(i+1)/2.0))+j-i-1

'''
    Pass 1
    Step 1. Change item names into integers 
    Step 2. Count support of items
'''
items = {} # key : item names value : its integers
items_inv = {} # key : its integers value : item names
counts = [] # Item counts
index = 0
for basket in baskets:
    basket = basket.split()
    basket = set(basket) # to remove duplication
    for item in basket:
        if item not in items: #new item - add to items dict
            items[item] = index # Step 1
            items_inv[index]= item # Step 1
            counts.append(1) # Step 2
            index += 1
        else: # existing item - add count
            counts[items[item]] += 1

'''
    Find frequent single item and reindexing
'''
support_th = 200

new_index = 0
for i in range (len(counts)):
    if counts[i] >= support_th: # freq. item
        freq_item = items_inv[i] 
        items[freq_item] = new_index # reindexing
        items_inv[new_index] = freq_item # reindexing
        new_index += 1
    else: # not freq. item
        not_freq_item = items_inv[i]  
        del items[not_freq_item] # removing
        del items_inv[i] # removing

# As a result, itmes contain only frequent singleton item
# items_inv is also updated. (still includes not freq. items, but doesn't matter)

'''
    Pass 2
    Step 1. Make all pairs of frequent singleton items
    Step 2. Count support of all pairs
    Step 3. Find frequent pairs and count their supports
'''
# Note that new_index = # of freq. items
# Step 1
tri_matrix = [] # 1-d list for triangular matrix
for i in range(new_index-1): 
    for j in range(i+1, new_index):
        tri_matrix.append(0)

# Step 2
for basket in baskets:
    basket = basket.split()
    for i in range (len(basket)):
        for j in range (i+1, len(basket)):
            if basket[i] == basket[j]: continue # two items are same
            
            if basket[i] in items and basket[j] in items:
                # Add 1 to count
                m = min((items[basket[i]],items[basket[j]]))
                M = max((items[basket[i]],items[basket[j]]))
                tri_matrix[getindex(m,M,new_index)] += 1

# Step 3
results = []
for i in range(new_index-1): 
    for j in range(i+1, new_index):
        if tri_matrix[getindex(i,j,new_index)] >= support_th:
            results.append(((i,j),tri_matrix[getindex(i,j,new_index)]))

results = sorted(results, key=lambda x:(-x[1], x[0][0], x[0][1])) 

# print the results
print(new_index) 
print(len(results))
temp = 0
for ((index1, index2), count) in results:
    if temp >= 10:
        break
    else:
        print(items_inv[index1]+'\t'+items_inv[index2]+'\t'+str(count))
        temp += 1

# end = time.time()
# print("Elapsed time is="+str(end-start))