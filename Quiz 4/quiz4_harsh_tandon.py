#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import lower, col, desc, lit, concat

#read inputs
n = int(sys.argv[1])
input_path = sys.argv[2]
df = spark.read.format('csv').options(header="true", inferSchema='true').load(input_path)

#select the columns to work on
df2 = df.select('NAMELAST','NAMEFIRST','visitee_namelast','visitee_namefirst')

count_1 = df2.count() #count of rows before filtering
df2 = df2.filter(df2.NAMELAST.isNotNull() & df2.visitee_namelast.isNotNull())
count_2 = df2.count() #count of rows after filter

#convert everything to lower case
for i in df2.columns:
    df2 = df2.withColumn(i, lower(col(i)))
    
#convert a temp view to use spark.sql
df2.createOrReplaceTempView("whitehouse_table")


# %%


#The 10 most frequent visitors to the White House
print("Top 10 most frequent visitors to the White House:\n")
spark.sql("SELECT concat(NAMELAST,' ',NAMEFIRST) as visitor, count(*) as frequency            FROM whitehouse_table            GROUP BY 1            ORDER BY 2 DESC            LIMIT 10").show()



# %%


#The 10 most frequent visitees to the White House
print("Top 10 most frequent visitees to the White House:\n")
spark.sql("SELECT concat(visitee_namelast,' ',visitee_namefirst) as visitee, count(*) as frequency            FROM whitehouse_table            GROUP BY 1            ORDER BY 2 DESC            LIMIT 10").show()



# %%


#The 10 most frequent visitors-vistees combinations
print("Top 10 most frequent visitor-visitee combinations:\n")
spark.sql("SELECT concat(NAMELAST,' ',NAMEFIRST) as visitor, concat(visitee_namelast,' ',visitee_namefirst) as visitee, count(*) as frequency            FROM whitehouse_table            GROUP BY 1,2            ORDER BY 3 DESC            LIMIT 10").show()



# %%


#The n most frequent visitors to the White House
print("Top", n, "most frequent visitors to the White House:\n")
spark.sql("SELECT concat(NAMELAST,' ',NAMEFIRST) as visitor, count(*) as frequency            FROM whitehouse_table            GROUP BY 1            ORDER BY 2 DESC").show(n)




# %%


#The n most frequent visitees to the White House
print("Top", n, "most frequent visitees to the White House:\n")
spark.sql("SELECT concat(visitee_namelast,' ',visitee_namefirst) as visitee, count(*) as frequency            FROM whitehouse_table            GROUP BY 1            ORDER BY 2 DESC").show(n)



# %%


#The n most frequent visitors-vistees combinations
print("Top", n, "most frequent visitor-visitee combinations:\n")
spark.sql("SELECT concat(NAMELAST,' ',NAMEFIRST) as visitor, concat(visitee_namelast,' ',visitee_namefirst) as visitee, count(*) as frequency            FROM whitehouse_table            GROUP BY 1,2            ORDER BY 3 DESC").show(n)



# %%


#The number of records dropped
print('Number of records dropped: ', (count_1-count_2))

#The number of records processed
print('Number of records processed: ', count_2)

