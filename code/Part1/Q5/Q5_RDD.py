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

spark = SparkSession.builder.appName("Q5_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity

startTime = time.time()

# ####### AUXILIARY FUNCTIONS ############

# 0. classic splitComma
def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# 1. auxiliary function to seperate again a custom made concat string
def splitUserGenre(x):
        user, genre = x[0].split(":")
        return (genre,(user,x[1]))

# 2. auxiliary function. Returns the same if genres are equal
def checkSameGenre(x):
        if x[1][1]==x[1][0][2]:
                return [(x[0],(x[1][0][0],x[1][0][1],x[1][0][2]))]
        else:
                return []
# 3. auxiliary function. Returns max tuple, with priority on the first element
def maxTuple(x,y):
        if float(x[1])<float(y[1]) or (x[1]==y[1] and float(x[2])<float(y[2])):
                return y
        else:
                return x

# 4. auxiliary function. Returns min tuple, with priority on the first element
def minTuple(x,y):
        if float(x[1])>float(y[1]) or (x[1]==y[1] and float(x[2])<float(y[2])):
                return y
        else:
                return x

# ####### MAIN ############

# A1. split ratings
# A2. create (userID, (movieID, rating))
movieID_userID_rating = \
        sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[0],(x[1],x[2])))


# A3. create (movieID, userID)
movieID_userID = movieID_userID_rating .\
        map(lambda x: (x[1][0],x[0]))

# ------------------------------ 

# B1. split movies_genres
# B2. create (movieID, genre)
movieID_genre = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[0], x[1]))

# C1. Join A.movieID_userID with B.movieID_genre
# C2. create (string(userID:genre), 1)
# C3. aggregate userID:genre total ratings
# C4. split string(userID:genre) to userID, genre
# C5. keep the userID, where totalRatings is max
# C6. create(userID, (genre, totalRatings)) genre is the key
userID_genre_MaxTotalRatings = movieID_userID.join(movieID_genre). \
        map(lambda x: (x[1][0]+":"+x[1][1],1)). \
        reduceByKey(lambda x,y: int(x)+int(y)). \
        map(lambda x: splitUserGenre(x)). \
        reduceByKey(lambda x,y: x if int(x[1])>int(y[1]) else y). \
        map(lambda x: (x[1][0],(x[0],x[1][1])))


# D1. Join C.userID_genre_MaxTotalRatings with A.movieID_userID_rating
# D2. create (movieID, (userID, rating, genre))
movieID_userID_rating_genre = userID_genre_MaxTotalRatings.join(movieID_userID_rating). \
        map(lambda x: (x[1][1][0],(x[0],x[1][1][1],x[1][0][0])))

# This is necessary due to multiple genre types
# E1. Join D.movieID_userID_rating_genre with B.movieID_genre
# E2. conditional create (movieID, (userID, rating, genre)) movie is the key
cond_movieID_userID_rating_genre = movieID_userID_rating_genre.join(movieID_genre). \
        flatMap(lambda x: checkSameGenre(x))

# ------------------------------ 

# F1. split movies
# F2. create (movieID, (title, popularity))
movieID_title_popularity = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x: splitComma(x)). \
        map(lambda x: (x[0],(x[1],x[7])))

# I1. join E.cond_movieID_userID_rating_genre with F.movieID_title_popularity
# I2. create (title, (rating, popularity))
title_rating_popularity = cond_movieID_userID_rating_genre.join(movieID_title_popularity). \
        map(lambda x: (x[1][0][2],(x[1][1][0],x[1][0][1],x[1][1][1])))

# I3. create (genre max(title, rating, popularity))
max_movie_ratings = title_rating_popularity .\
        reduceByKey(lambda x,y: maxTuple(x,y))

# I4. create (genre min(title, rating, popularity))
min_movie_ratings = title_rating_popularity .\
        reduceByKey(lambda x,y: minTuple(x,y))

# L1. create (genre, (userID, maxTotalRatings))
genre_user_maxTotalRatings = userID_genre_MaxTotalRatings. \
        map(lambda x: (x[1][0],(x[0],x[1][1])))

# L2. create (genre, ((max_movie_ratings), (min_movie_ratings)))
max_min_movie_ratings = max_movie_ratings.join(min_movie_ratings). \
        map(lambda x: (x[0],(x[1][0],x[1][1])))

# ------------------------------ 

# N1. join L1 and L2
# N2. create final tuple (see print below) and sort it
rdd = genre_user_maxTotalRatings.join(max_min_movie_ratings). \
        map(lambda x: (x[0], (x[1][0][0],x[1][0][1],x[1][1][0][0],x[1][1][0][1],x[1][1][1][0],x[1][1][1][1]))). \
        sortByKey(ascending=True)


print("(Eidos, (Christis me perissoteres kritikes, Plithos Kritikon, Perissotero Agapimeni Tainia, Vathmologia Perissotero Agapimenis Tainias, Ligotero Agapimeni Tainia, Vathmologia Ligotero Agapimenis Tainias))")

for i in rdd.collect():
        print(i)


endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")
