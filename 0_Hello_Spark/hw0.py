'''
Assignment #0
hw0.py
Print the number of words that start with each letter (alphabetical order)
Created by 20200678 GyoukChu on 2022/09/16
Last Updated on 2022/09/21
'''
import re
import sys
from pyspark import SparkConf, SparkContext
from string import ascii_lowercase

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1]) # Read a file into RDD

words = lines.flatMap(lambda l:re.split(r'[^\w]+', l)) # Parse into words 

# Check followings:
# 1. words is not empty
# 2. the first character is alphabet
valwords = words.filter(lambda w:w!='' and w[0].isalpha())

lowerwords = valwords.map(lambda w: w.lower()) # Make all words as lowercase
testwords = lowerwords.distinct() # Consider "unique" words

# key : the first character w/ lower case. value : 1
pairs = testwords.map(lambda w: (w[0],1))

counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).sortByKey() # Adding values

results = counts.collect()

# print the results
temp = 0
for c,cnt in results:
	while c!=ascii_lowercase[temp]:
		print(ascii_lowercase[temp]+'\t'+'0')
		temp +=1
	print(c+'\t'+str(cnt))
	temp +=1
while temp < len(ascii_lowercase):
	print(ascii_lowercase[temp]+'\t'+'0')
	temp += 1
