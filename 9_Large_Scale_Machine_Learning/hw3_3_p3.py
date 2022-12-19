'''
    Assignment #3
    hw3_3.py

    Created by 20200678 GyoukChu on 2022/11/15
    Last Updated on 2022/11/25
'''
# Note: Only import followings;
# pyspark, re, sys, math, csv, time, collections,
# itertools, os, and numpy

import sys
import numpy as np
# import time
# start = time.time()

def mysplit_feature(lines):
    '''
        Split the given string as decribed below.

        Input : list of strs

        Output: list of lists, consist of features
        Note that for bias, add 1 at the end of feature vector
    '''
    result = []
    for line in lines:
        feature = line.split(',')
        feature = list(map(float, feature))
        feature.append(1.0)
        feature = np.array(feature)
        result.append(feature)
    return result

def mysplit_label(lines):
    '''
        Split the given string as decribed below.

        Input : list of strs

        Output: list of lists, consist of a label
    '''
    result = []
    for line in lines:
        label = float(line)
        label = np.array(label)
        result.append(label)
    return result

def k_fold_split(data, k):
    '''
        Split a list into k chunks of same size.
        it is for k-fold cross validation.
        Note that length of data must be divisible by k.

        Input: list of lists,
                number of chunks

        Output: list of k lists(chunks)
    '''
    result = []
    m = int(len(data)/k)
    for i in range(k):
        temp = data[i*m:(i+1)*m]
        result.append(temp)
    
    return result

class SVM:
    '''
        SVM model
    '''
    def __init__(self, feature_size):
        # Weight Initialization - w/ normal distribution.
        self.W = np.random.normal(loc=0, scale=0.1, size=(feature_size,1))

    def forward(self, feature, label, C, eta):
        # feature - (# of data, feature_size)
        # label - (# of data, 1)
        # Forward propagation - skip since we don't need loss value
        # Backpropagation
        criterion = np.multiply(np.matmul(feature, self.W), label)<1
        # criterion - (# of data, 1)
        temp = np.matmul(np.transpose(feature),np.multiply(criterion, -label))
        # temp - (feature_size, 1)
        grad = self.W + C * temp
        # Weight Update
        self.W = self.W - eta * grad

        return 
    
    def predict(self, feature):
        # feature = (# of data, feature_size)
        check = np.matmul(feature, self.W)
        return np.sign(check) + (check==0)

# main

with open(sys.argv[1], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))
data_feature = mysplit_feature(lines)

with open(sys.argv[2], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))
data_label = mysplit_label(lines)

k = 10
feature_size = 122 + 1
C = 0.01 # loss, hyperparameter
eta = 0.01 # learning rate, hyperparameter
num_iter = 1000

data_feature = k_fold_split(data_feature, k)
data_label = k_fold_split(data_label, k)

accuracy_list = []

for _ in range(k):
    train_data_feature = data_feature[1:k]
    train_data_label = data_label[1:k]
    val_data_feature = data_feature[0:1]
    val_data_label = data_label[0:1]
    train_data_feature = np.concatenate(train_data_feature)
    train_data_label = np.concatenate(train_data_label).reshape(-1,1)
    val_data_feature = np.concatenate(val_data_feature)
    val_data_label = np.concatenate(val_data_label).reshape(-1,1)
    # Change the order
    temp1 = data_feature[0]
    temp2 = data_label[0]
    for i in range(k-1):
        data_feature[i] = data_feature[i+1]
        data_label[i] = data_label[i+1]
    data_feature[k-1] = temp1
    data_label[k-1] = temp2
    
    model = SVM(feature_size)
    for _ in range(num_iter):
        model.forward(train_data_feature, train_data_label, C, eta)
    
    prediction = model.predict(val_data_feature)
    acc = np.sum(np.isclose(val_data_label, prediction))/len(val_data_label)
    accuracy_list.append(acc)

print(sum(accuracy_list)/len(accuracy_list))
print(C)
print(eta)
# end = time.time()
# print("Elapsed time is="+str(end-start))   
# Run: python hw3_3_p3.py features.txt labels.txt