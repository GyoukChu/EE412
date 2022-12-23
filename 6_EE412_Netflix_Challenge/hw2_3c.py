'''
    This is the code of Choi Hyukmoon, a student in the same course.
    My code to implement UV-decomposition had an error, so I referenced this.
    Thanks to him :)
'''

import sys
import math
import numpy as np

f1 = open(sys.argv[1], 'r')
f2 = open(sys.argv[2], 'r')
f1_lines = f1.readlines()
f2_lines = f2.readlines()

user_list = set()      #list of users
movie_list = set()    #list of movies

for i in range(len(f1_lines)):
    f1_lines[i] = f1_lines[i].split(',')


for i in range(len(f2_lines)):
    f2_lines[i] = f2_lines[i].split(',')

for l in f1_lines:
    user_list.add(l[0])
    movie_list.add(l[1])


for l in f2_lines:
    user_list.add(l[0])
    movie_list.add(l[1])


user_list = list(user_list)
movie_list = list(movie_list)

N_users = len(user_list)    
N_movies = len(movie_list)  

user_dict = dict()      #user_dict[U] = index of U in user_list
movie_dict = dict()     #movie_dict[M] = index of M in movie_list

for i in range(N_users):
    user_dict[user_list[i]] = i

for j in range(N_movies):
    movie_dict[movie_list[j]] = j

#Computation of the utility matrix; Utility[i, j] = rating of user_list[i] to movie_list[j]

Utility = np.zeros((N_users, N_movies))

for l in f1_lines:
    Utility[user_dict[l[0]], movie_dict[l[1]]] = float(l[2])

Utility[Utility == 0] = np.nan

#Preprocessing

average_user = np.empty((N_users, 1))

for i in range(N_users):   
    average_user[i, 0] = np.nanmean(Utility[i, :])

average_user[np.isnan(average_user)] = 0

Utility -= np.dot(average_user, np.ones((1, N_movies)))

average_movie = np.empty((1, N_movies))

for j in range(N_movies):   
    average_movie[0, j] = np.nanmean(Utility[:, j])

average_movie[np.isnan(average_movie)] = 0

Utility -= np.dot(np.ones((N_users, 1)), average_movie)


#Initialization; U: N_users-by-k matrix, V: k-by-N_movies matrix, length = # of (U, V) pairs

k = 4
length = 10

U = []
V = []

for i in range(length):
    U.append(np.random.rand(N_users, k))
    norm = math.sqrt(np.trace(np.dot(U[i].transpose(), U[i])))
    U[i] = math.sqrt(1/k) * np.ones((N_users, k)) + U[i] / norm 

    V.append(np.random.rand(k, N_movies))
    norm = math.sqrt(np.trace(np.dot(V[i], V[i].transpose())))
    V[i] = math.sqrt(1/k) * np.ones((k, N_movies)) + V[i] /norm


#Root Mean Square Error(RMSE)

def RMSE(U, V):        
    M = Utility - np.dot(U, V)
    condition = np.isnan(M)
    M[condition] = 0
    N = np.count_nonzero(condition == False)
    return math.sqrt(np.trace(np.dot(M, M.transpose())) / N)

#Optimization.

def optimization(U, V):

    RMSE1 = RMSE(U, V)     #RMSE before optimization

    for r in range(N_users):
        for s in range(k):
            v = np.array(V[s, :])
            v[np.isnan(Utility[r, :])] = 0
            Denominator = np.dot(v, v)
            Numerator = Utility[r, :] - np.dot(U[r, :], V) + np.dot(U[r, s], v)
            Numerator[np.isnan(Numerator)] = 0
            Numerator = np.dot(Numerator, v)

            if Denominator != 0:
                U[r, s] = Numerator / Denominator
            else: U[r, s] = 0
    
    for r in range(k):
        for s in range(N_movies):
            u = np.array(U[:, r])
            u[np.isnan(Utility[:, s])] = 0
            Denominator = np.dot(u, u)
            Numerator = Utility[:, s] - np.dot(U, V[:, s]) + np.dot(u, V[r, s])
            Numerator[np.isnan(Numerator)] = 0
            Numerator = np.dot(u, Numerator) 

            if Denominator != 0:
                V[r, s] = Numerator / Denominator
            else: V[r, s] = 0
    
    RMSE2 = RMSE(U, V)     #RMSE after optimization

    return RMSE1 - RMSE2


for i in range(length):
    RMSE_decrease = optimization(U[i], V[i])
    while(RMSE_decrease > 0.01):
        RMSE_decrease = optimization(U[i], V[i])
        print(RMSE(U[i], V[i]))


U_average = np.zeros((N_users, k))
V_average = np.zeros((k, N_movies))

for i in range(length):
    U_average += U[i]
    V_average += V[i]

U_average = U_average / length
V_average = V_average / length

M = np.dot(U_average, V_average)


result = open("output.txt", 'w')

for l in f2_lines:
    user = user_dict[l[0]]
    movie = movie_dict[l[1]]
    rating = M[user, movie] +  average_user[user, 0] + average_movie[0, movie]
    if rating > 5:
        rating = 5
    elif rating < 0:
        rating = 0
    result.write("{},{},{},{}".format(l[0], l[1], rating, l[3]))


f1.close()
f2.close()
result.close()
