'''
    Assignment #1
    hw1_3.py

    Created by 20200678 GyoukChu on 2022/10/04
    Last Updated on 2022/10/05
'''
# Note: Only import followings;
# pyspark, numpy, re, sys, math, csv, and os
import sys
import math
import re
import numpy as np

# import time
# start = time.time()

def extract_shingles(k, text):
    '''
        Extract k-shingles from text
        
        Input: k - shingle size
               text - string
        Output: set of k-shingles
        Here, use set to remove the repetition of same shingles
    '''
    shingles = set()
    for i in range(len(text)-k+1):
        shingles.add(text[i:i+k])
    return shingles

def is_not_prime(n):
    '''
        Check whether input number is prime or not
        
        Input: n
        Output: True if is not prime. False O.W.
    '''
    for i in range(2,int(math.sqrt(n))+1):
        if (n%i) == 0:
            return True
    return False

def larger_prime(n):
    '''
        Return the smallest prime number larger than or equal to n

        Input: n
        Output: Integer explained above.
    '''
    c=n
    while is_not_prime(c): c+=1
    return c


def rand_hashfunc(n):
    '''
        Return the given hash function
        
        Input: n
        Output: hash function (ax + b) % c 
    '''
    c = larger_prime(n)
    a = np.random.randint(0,c-1)
    b = np.random.randint(0,c-1)
    return lambda x:(a*x+b)%c

def minhash(ID_and_shingles,n,b,r):
    '''
        Return the signature matrix
        
        Input: ID_and_shingles:dict s.t.
                    key: ID, value:set of shingles indices
               n: # of whole shingles
               b: # of bands
               r: # of rows
        (b*r) is the number of hash functions to apply.
        
        Output: signiture matrix with size (b*r) x (# of ids)
        A List of (b*r) many lists. 
        Each list consists of minhashes of each id
    '''
    sig_matrix = []
    # Apply hash functions (b*r) times for each id
    for i in range(b*r):
        signature = []
        temp_func = rand_hashfunc(n)
        for id in ID_and_shingles:
            shingles = ID_and_shingles[id]
            minhashcode = larger_prime(n) + 1
            for shingle in shingles:
                hashcode = temp_func(shingle)
                if hashcode < minhashcode:
                    minhashcode = hashcode
            signature.append(minhashcode)
        sig_matrix.append(signature)

    return sig_matrix

def LSH(sig_matrix,ID_and_shingles,threshold,b,r):
    '''
        Return candidate pairs whose signature components 
            are same by at least threshold
        
        Input: sig_matrix: signature matrix by minhash func.
               ID_and_shingles: dict s.t.
                    key: ID, value:set of shingles indices
               threshold: threshold.
               b: # of bands
               r: # of rows

        Output: list of candidate pairs

        Here, we use hash function for each band as like identity function
        (i.e. minhashes of each band are diff., then hashed value is also diff.)
        So, hashing band is unnecessary process here, 
        but can use other hash function with simple edit :)
    '''
    candidates = []
    band_dict = {}
    index = 0
    for (i, ID1) in enumerate(ID_and_shingles):
        signature1 = []
        for sig in sig_matrix:
            signature1.append(sig[i]) # extract minhash signatures of ID1
        for (j, ID2) in enumerate(ID_and_shingles):
            if j<=i : continue # same or already considered
            signature2 = []
            for sig in sig_matrix:
                signature2.append(sig[j]) # extract minhash signatures of ID2
            
            # For each band, compare that portion of minhashes
            for k in range(b):
                band_hash1 = signature1[r*k:r*(k+1)]
                if str(band_hash1) not in band_dict:
                    band_dict[str(band_hash1)] = index
                    index += 1
                band_hash2 = signature2[r*k:r*(k+1)]
                if str(band_hash2) not in band_dict:
                    band_dict[str(band_hash2)] = index
                    index += 1   
                # If hashed value of bands are same, then they become candidate
                if band_dict[str(band_hash1)] == band_dict[str(band_hash2)]:
                    candidates.append((ID1,ID2) if int(ID1[1:])<=int(ID2[1:]) \
                                                else (ID2,ID1))
                    break 
    return candidates

# Parameters   
k = 3
b = 6
r = 20
threshold = 0.9

ID_and_shingles = {} # key:ID, value:set of shingles indices
all_shingles = {} # key:shingle, value:index
all_shingles_inv = {} # key:index, value:shingle
index = 0

f = open(sys.argv[1],"r",encoding='UTF-8') # file read
lines = f.readlines()

for line in lines:
    line = line.split(' ',1)
    id = line[0]
    text = line[1]
    text = re.sub(r'[^a-zA-Z\s]','',text)
    # Ignore non-alphabet characters except the white space
    text = text.lower() # Convert all characters to lower case
    
    shingles = extract_shingles(k, text)

    shingles_indices = set()
    # Add key-value pairs for ID_and_shingles, all_shingles
    for shingle in shingles:
        if shingle not in all_shingles: # new shingle
            all_shingles[shingle] = index
            all_shingles_inv[index] = shingle
            shingles_indices.add(index)
            index += 1
        else:
            shingles_indices.add(all_shingles[shingle])
    
    ID_and_shingles[id]=shingles_indices

sig_matrix = minhash(ID_and_shingles,len(all_shingles),b,r)

candidates = LSH(sig_matrix,ID_and_shingles,threshold,b,r)

# print the results
for (id1, id2) in candidates:
    print(str(id1)+'\t'+str(id2))

# end = time.time()
# print("Elapsed time is="+str(end-start))