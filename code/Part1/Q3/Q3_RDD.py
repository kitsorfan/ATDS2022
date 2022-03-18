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

spark = SparkSession.builder.appName("Q3_RDD").getOrCreate()
sc = spark.sparkContext

startTime = time.time()

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# A1. Split ratings
# A2. create (movieID, (rating, 1))
# A3. aggregate (totalRatingValue, totalNumberOfRatings)
# A4. compute meanRatingPerMovie = totalRatingValue/totalNumberOfRatings
meanRatingPerMovie = \
        sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[1],(x[2],1))). \
        reduceByKey(lambda x,y:  (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        map(lambda x: (x[0], float(x[1][0])/int(x[1][1])))

# B1. Split movies genres
# B2. create (movieID, genre)
movieGenres = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[0],x[1]))

# C1. join meanRatingPerMovie with movieGenres
# C2. create (genre, (meanRatingPerMovie, 1))
# C3. aggregate (totalRatingPerGenre, totalMoviesPerGenre)
# C4. create (genre, meanRatingPerGenre = round(totalRatingPerGenre/totalMoviesPerGenre), totalMoviesPerGenre)
rdd = meanRatingPerMovie.join(movieGenres). \
        map(lambda x: (x[1][1],(x[1][0],1))). \
        reduceByKey(lambda x,y: (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        map(lambda x: (x[0],(round(float(x[1][0])/int(x[1][1]),3),x[1][1])))


for i in rdd.collect():
        print(i)

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")
