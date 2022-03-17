# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

from pyspark.sql import SparkSession

from io import StringIO
import csv
import time

"""
movie_genres.csv
0-movieID, 1-genre

movies.csv
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

spark = SparkSession.builder.appName("Q1_RDD").getOrCreate()

sc = spark.sparkContext
startTime = time.time()

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def check(x):
        title = x[1]
        year = x[3][0:4]
        cost = int(x[5])
        revenue = int(x[6])
        if year>="2000" and cost != 0 and revenue != 0:
                return [(year,(title, ((revenue-cost)/cost)*100))]
        else:
                return []

rdd = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x: splitComma(x)). \
        flatMap(lambda x: check(x)). \
        reduceByKey(lambda x,y:  (x if x[1]>y[1] else y)). \
        sortBy(lambda x: x[0], ascending=False)
        
for i in rdd.collect():
        print(i)

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")
