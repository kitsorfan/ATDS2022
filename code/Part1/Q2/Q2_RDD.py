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
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue, 7-popularity

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

spark = SparkSession.builder.appName("Q2_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity

startTime = time.time()

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]


# we seperate "good" from "bad" users. We count all because we need percentage
def seperator(x):
        meanRating = float(x[1][0])/int(x[1][1])
        if meanRating>3.0:
                return [(1,(1,1))]
        else:
                return [(1,(0,1))] 

# 1. Split comma
# 2. create (userID, (rating, 1))
# 3. aggregate (userID, totalRatingValue, totalRatings)
# 4. create (1,(1,1)) for "good", (1,(0,0)) for "bad"    (userID in now considered same for all)
# 5. aggregate "good"s and "bad"s
# 6. calculate percentage
rdd = \
        sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[0],(x[2],1))). \
        reduceByKey(lambda x,y:  (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        flatMap(lambda x: seperator(x)). \
        reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])). \
        map(lambda x: (x[1][0]/x[1][1])*100)


for i in rdd.collect(): # single value
        print(str(round(i,3))+"%")

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")
