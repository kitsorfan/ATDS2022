# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

from pyspark.sql import SparkSession

import time

"""
movie_genres.csv
0-movieID, 1-genre

movies.csv
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

spark = SparkSession.builder.appName("Q2_SQL_csv").getOrCreate()

startTime = time.time()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
ratings.createOrReplaceTempView("ratings")

query1 = """
        SELECT 
                AVG(_c2) as meanRating, 
                _c0 AS user 
        FROM ratings 
        GROUP BY _c0
        """
meanUserRating = spark.sql(query1)
meanUserRating.createOrReplaceTempView("meanUserRating")

all_users = "SELECT COUNT(*) FROM meanUserRating"

query2 = """
        SELECT (COUNT(*)/("""+all_users+""")*100) AS goodUsers 
        FROM meanUserRating AS mru 
        WHERE mru.meanRating>3.0"""

sqlQ2 = spark.sql(query2)
sqlQ2.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")