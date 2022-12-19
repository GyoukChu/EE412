'''
    Assignment #3
    hw3_2.py

    Created by 20200678 GyoukChu on 2022/11/15
    Last Updated on 2022/11/24
'''
# Note: Only import followings;
# pyspark, re, sys, math, csv, time, collections,
# itertools, os, and numpy
import sys
from itertools import combinations
#import time
#start = time.time()

def mysplit(lines):
    '''
        Split the given string as decribed below.

        Input : list of strings,
        each string(line) : <user_id1> <user_id2> <time_stamp> (between them, there is whitespace char.)

        Output: list of pairs, (user_id_min, user_id_max)
    '''
    result = []
    for line in lines:
        user_id1, user_id2, _ = line.split()
        user_id1 = int(user_id1)
        user_id2 = int(user_id2)
        result.append((min(user_id1, user_id2), max(user_id1, user_id2)))
    return result

def left_islessthan_right(node1, degree1, node2, degree2):
    '''
        Compare two nodes under certain condition;
        v<u iff 1. degree(v)<degree(u) or 2. degrees are same, but v<u

        Input: (node1, degree1, node2, degree2)
        Output: True if node1<node2, False otherwise.
    '''
    if degree1 < degree2:
        return True
    elif degree1 == degree2:
        if node1 < node2:
            return True
    return False

def heavy_hitter_nodes(node_degrees, limit):
    result = []
    for node in node_degrees:
        if node_degrees[node] >= limit:
            result.append(node)
    return result

def extract_data(node_pairs):
    '''
        From pairs of nodes, make a list of 3 data

        Input : list of pairs, (user_id_min, user_id_max)

        Output: list of 4 data described below
            node_degrees: dict which key is each node, and value is key node's degree
            edge_index_pair: dict which key is pair of nodes, and value is 1 (no meaning)
            edge_index_single: dict which key is each node, and value is the list of nodes
                                that connect with key node
            heavy_hitter_nodes_list: list of heavy hitter nodes
    '''
    node_degrees = {}
    edge_index_pair = {}
    edge_index_single = {}
    
    node_pairs.sort()
    for (node1, node2) in node_pairs:
        # For node_degrees
        if node1 not in node_degrees:
            node_degrees[node1] = 1
        else:
            node_degrees[node1] += 1
        if node2 not in node_degrees:
            node_degrees[node2] = 1
        else:
            node_degrees[node2] += 1      
        
        # For edge_index_pair
        if (node1, node2) not in edge_index_pair:
            edge_index_pair[(node1, node2)] = 1
        # else: not happened
        
        # For edge_index_single
        if node1 not in edge_index_single:
            edge_index_single[node1] = [node2]
        else:
            edge_index_single[node1].append(node2)
        if node2 not in edge_index_single:
            edge_index_single[node2] = [node1]
        else:
            edge_index_single[node2].append(node1)
    # For heavy_hitter_nodes_list
    num_edges = len(edge_index_pair)
    heavy_hitter_nodes_list = heavy_hitter_nodes(node_degrees, pow(num_edges, 0.5))
    
    return [node_degrees, edge_index_pair, edge_index_single, heavy_hitter_nodes_list]

def count_heavy_hitter_triangles(extracted_data):
    '''
        Count heavy-hitter traingles.

        Input: list of 4 data described below
            node_degrees: dict which key is each node, and value is key node's degree
            edge_index_pair: dict which key is pair of nodes, and value is 1 (no meaning)
            edge_index_single: dict which key is each node, and value is the list of nodes
                                that connect with key node
            heavy_hitter_nodes_list: list of heavy hitter nodes

        Output: int, number of triangles
    '''
    result = 0

    [_, edge_index_pair,_, heavy_hitter_nodes_list] = extracted_data

    for (node1, node2, node3) in combinations(heavy_hitter_nodes_list, 3):
        if (node1, node2) not in edge_index_pair:
            continue
        elif (node1, node3) not in edge_index_pair:
            continue
        elif (node2, node3) not in edge_index_pair:
            continue
        else:
            result += 1
    return result

def count_other_triangles(extracted_data):
    '''
        Count non heavy-hitter traingles.

        Input: list of 4 data described below
            node_degrees: dict which key is each node, and value is key node's degree
            edge_index_pair: dict which key is pair of nodes, and value is 1 (no meaning)
            edge_index_single: dict which key is each node, and value is the list of nodes
                                that connect with key node
            heavy_hitter_nodes_list: list of heavy hitter nodes

        Output: int, number of triangles
    '''
    result = 0

    [node_degrees, edge_index_pair, edge_index_single, heavy_hitter_nodes_list] = extracted_data
    candidate_edges = []
    for (node1, node2) in edge_index_pair:
        if (node1 in heavy_hitter_nodes_list) and (node2 in heavy_hitter_nodes_list):
            continue
        else:
            if left_islessthan_right(node1, node_degrees[node1], node2, node_degrees[node2]):
                candidate_edges.append((node1, node2))
            else:
                candidate_edges.append((node2, node1))
    for (node1, node2) in candidate_edges:
        adjacent_nodes = edge_index_single[node1].copy()
        adjacent_nodes.remove(node2) # always exist
        for node3 in adjacent_nodes:
            if (min(node2, node3), max(node2, node3)) not in edge_index_pair:
                continue
            elif not left_islessthan_right(node2, node_degrees[node2], node3, node_degrees[node3]):
                continue
            else:
                result +=1
    
    return result

# main

with open(sys.argv[1], 'r') as file:
    lines = file.readlines()
    lines = list(map(lambda s: s.strip('\n'), lines))

node_pairs = mysplit(lines)
node_pairs = list(set(node_pairs)) # remove duplicates

extracted_data = extract_data(node_pairs)
result1 = count_heavy_hitter_triangles(extracted_data)
result2 = count_other_triangles(extracted_data)

total = result1 + result2
print(total)

# Run: python hw3_2_p3.py facebook-links.txt
# end = time.time()
# print("Elapsed time is="+str(end-start))