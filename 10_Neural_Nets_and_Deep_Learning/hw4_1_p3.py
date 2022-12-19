'''
    Assignment #4
    hw4_1_p3.py

    Created by 20200678 GyoukChu on 2022/12/10
    Last Updated on 2022/12/17
'''
# Note: Only import followings;
# numpy, sys, math, csv, os

import sys
import numpy as np

def mysplit(line):
    feature_list = []
    label_list = []
    for line in lines:
        data = line.split(',')
        feature, label = data[:-1], data[-1]
        for i in range(len(feature)):
            feature[i] = float(feature[i])
        label = float(label)
        
        feature = np.array(feature)
        label = np.array(label)
    
        feature_list.append(feature)
        label_list.append(label)
        
    return np.array(feature_list, dtype=np.float128), np.array(label_list, dtype=np.float128)

def one_hot_encoding(label_list):
    new_label_list = []
    for i in range(len(label_list)):
        new_label = np.zeros(10)
        temp = int(label_list[i])
        new_label[temp] = 1
        new_label_list.append(new_label)
    return np.array(new_label_list)

def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))

class Fully_Connected_Layer:
    def __init__(self, learning_rate, momentum=0.9):
        self.InputDim = 784
        self.HiddenDim = 128
        self.OutputDim = 10
        self.learning_rate = learning_rate
        self.momentum = momentum # For momentum value

        '''Weight Initialization'''
        self.W1 = np.random.normal(loc=0.0, scale=0.1, size=(self.InputDim, self.HiddenDim))
        self.W2 = np.random.normal(loc=0.0, scale=0.1, size=(self.HiddenDim, self.OutputDim))
        self.dW1 = np.zeros((self.InputDim, self.HiddenDim)) # For momentum term
        self.dW2 = np.zeros((self.HiddenDim, self.OutputDim)) # For momentum term

    def Forward(self, Input):
        '''Implement forward propagation'''
        self.s1 = sigmoid(np.matmul(Input, self.W1)) # (# of data) x 128
        Output = sigmoid(np.matmul(self.s1, self.W2)) # (# of data) x 10
        return Output
    
    def Backward(self, Input, Label, Output):
        '''Implement backward propagation'''
        '''Update parameters using gradient descent'''
        # Using MSE loss here.
        # 1. Update output layer weight
        gradient = (Output-Label) * Output * (1-Output) # (# of data) x 10
        dW2 = np.matmul(self.s1.T, gradient) # 128 x 10
        self.W2 = self.W2 - self.learning_rate * (self.momentum * self.dW2 + (1-self.momentum) * dW2)
        self.dW2 = dW2

        # 2. Update hidden layer weight
        gradient = np.matmul(gradient, self.W2.T) # (# of data) x 128
        dW1 = np.matmul(Input.T, gradient) # 784 x 128
        self.W1 = self.W1 - self.learning_rate * (self.momentum * self.dW1 + (1-self.momentum) * dW1)
        self.dW1 = dW1
    
    def Train(self, Input, Label):
        Output = self.Forward(Input)
        self.Backward(Input, Label, Output)           

# main
learning_rate = 1.0
momentum = 0.9
iteration = 50

# For training data
with open(sys.argv[1], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))
train_feature_list, train_label_list = mysplit(lines)

# For testing data
with open(sys.argv[2], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))
test_feature_list, test_label_list = mysplit(lines)

train_label_list = one_hot_encoding(train_label_list)
test_label_list = one_hot_encoding(test_label_list)

'''Construct a fully-connected network'''        
Network = Fully_Connected_Layer(learning_rate, momentum)

'''Train the network for the number of iterations'''
'''Implement function to measure the accuracy'''
indices = list(range(len(train_feature_list)))
for i in range(iteration):
    Network.learning_rate = Network.learning_rate * 0.99 
    ### Sligtly decrease learning rate as training goes by. ###
    for index in indices:
        Network.Train(train_feature_list[index][np.newaxis,:], train_label_list[index][np.newaxis,:])
''' Accuracy for training dataset '''
train_predict = Network.Forward(train_feature_list)
training_accuracy = np.mean(np.argmax(train_predict, axis=1) == np.argmax(train_label_list, axis=1))

''' Accuracy for testing dataset '''
test_predict = Network.Forward(test_feature_list)
test_accuracy = np.mean(np.argmax(test_predict, axis=1) == np.argmax(test_label_list, axis=1))

print(training_accuracy)
print(test_accuracy)
print(iteration)
print(learning_rate)

# Run : python3 hw4_1_p3.py training.csv testing.csv