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

spark = SparkSession.builder.appName("Q4_RDD").getOrCreate()
sc = spark.sparkContext

startTime = time.time()

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def isDrama(x):
        movieID = x[0]
        genre = x[1]
        if genre=="Drama":
                return [(movieID, genre)]
        else:
                return []

def timePeriod(year):
        if 2000<=year and year<2005:
                return "2000-2004"
        elif 2005<=year and year<2010:
                return "2005-2009"
        elif 2010<=year and year<2015:
                return "2010-2014"
        elif 2015<=year and year<2020:
                return "2015-2019"

def countWordsTimePeriod(x):
        movieID = x[0]
        yearStr = (x[3][0:4])
        if (yearStr==''): yearStr="0"
        year = int(yearStr)
        description = x[2]
        if (description ==  '') or (year<2000):
                return []
        else:
                return [(movieID,(timePeriod(year),len(description.split())))]

# A1. split movies
# A2. create (movieID, (timePeriod, countWords))
moviePeriodWords = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x: splitComma(x)). \
        flatMap(lambda x: countWordsTimePeriod(x))

# B1. split movie_genre
# B2. create (movieID, genre) if drama, else ()
movieDrama = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x: splitComma(x)). \
        flatMap(lambda x: isDrama(x))


# C1. join moviePeriodWords with movieDrama
# C2. create (timePeriod, (countWords, 1))
# C3. aggregate (totalWordsPerPeriod, totalMoviesPerPeriod)
# C4. calculate meanDescriptionWordsPerPeriod = totalWordsPerPeriod/totalMoviesPerPeriod
rdd = moviePeriodWords.join(movieDrama). \
        map(lambda x: (x[1][0][0],(x[1][0][1],1))). \
        reduceByKey(lambda x,y: (int(x[0])+int(y[0]),int(x[1])+int(y[1]))). \
        map(lambda x: (x[0],round(int(x[1][0])/int(x[1][1]),3)))

for i in rdd.collect():
        print(i)

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")
