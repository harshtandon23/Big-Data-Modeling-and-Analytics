#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.mllib.stat import Statistics


#input_path = 'D:/3rd Qtr Study Material/Big Data Modeling/Quiz/customers_books.txt'
input_path = sys.argv[1]
recs = spark.sparkContext.textFile(input_path)
#recs.take(5)
# ['u1:book1', 'u1:book2', 'u1:book2', 'u1:book3', 'u1:book3']


split_recs = recs.map(lambda x: x.split(":"))
#split_recs.take(5)
# [['u1', 'book1'], ['u1', 'book2'], ['u1', 'book2'], ['u1', 'book3'], ['u1', 'book3']]


# To work with just numbers, lets convert book1 to numeric 1, book2 to numeric 2..and likewise for each book. 
split_recs_int = split_recs.map(lambda x: (x[0],int(x[1].split("k")[1])))
#split_recs_int.take(5)
# [('u1', 1), ('u1', 2), ('u1', 2), ('u1', 3), ('u1', 3)]


# Find all unique books bought by each user
books_rated_by_each_user = split_recs_int.groupByKey().mapValues(lambda x: set(x))
#books_rated_by_each_user.take(2)
# [('u5', {2, 3, 5, 9}), ('u6', {0, 1, 2, 3, 5, 9})]


# Find list of all unique books present in the dataset. This can be used to map whether a particular user bought a book or not.
unique_books = split_recs.map(lambda x: int(x[1].split("k")[1])).distinct().sortBy(lambda x: x).collect()
# [0, 1, 2, 3, 4, ....23, 31, 61]


def bought_or_not(k, v):
    bought = []
    for i in unique_books:
        if i in v:
            bought.append(1)
        else:
            bought.append(0)
    return (k, bought)

# For each user, find the list of books they bought or not
bought = books_rated_by_each_user.map(lambda x: bought_or_not(x[0], x[1])).values()
#bought.take(2)
# [[0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], [1, 1, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]


# Find correlations
correlations = Statistics.corr(bought, method='pearson')
# convert the list of uniquebooks to rdd 
uni_book = spark.sparkContext.parallelize(unique_books)
# convert the correlations to rdd
cors = spark.sparkContext.parallelize(correlations).map(lambda x: list(x))
#cors = corrlns.map(lambda x: list(x))

# create key value pairs of unique books and their list of correlations
pairs = uni_book.zip(cors)
#pairs.take(2)
# [(0, [1.0, -0.10050378152592108, 0.3892494720807616,-0.10050378152592108,.....]), (1, [-0.10050378152592108,1.0,...])]

def cartesian_dict_pairs(K, v): 
    di = dict(zip(unique_books, v))
    del di[K]
    di2 = sorted(di.items(), key=lambda x: x[1], reverse=True)
    return di2

#for each correlation value, assign a key (the book_ID the value corresponds to). Then sort the pairs in desceding order.
pairs_comb = pairs.map(lambda x: (x[0], cartesian_dict_pairs(x[0], x[1])))
print("Book wise Correlations:\n")
for i in pairs_comb.collect():
    print(i)
    

# extract top-2 key-value pairs (book_id, correlation_value) for each key (book_id)
ans = pairs_comb.map(lambda x: "book" + str(x[0]) + ": book" + str(x[1][0][0]) +", book"+ str(x[1][1][0]))
print("Customers who bought X: also bought X1, X2\n")
for i in ans.collect():
    print(i)


# In[ ]:


spark.sparkContext.stop()

