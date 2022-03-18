# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

from pyspark.sql import SparkSession

import time

"""
movie_genres.csv
0-movieID, 1-genre

movies.csv
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue, 7-popularity

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

spark = SparkSession.builder.appName("Q3_SQL_csv").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity

startTime = time.time()

movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

movie_genres.createOrReplaceTempView("movie_genres")
ratings.createOrReplaceTempView("ratings")

query1 = """
        SELECT 
                AVG(r._c2) AS meanRating, 
                r._c1 AS movieID 
        FROM ratings AS r
        GROUP BY r._c1
"""
ratingsPerMovie = spark.sql(query1)
ratingsPerMovie.createOrReplaceTempView("ratingsPerMovie")

query2 = """
        SELECT 
                rpm.meanRating AS meanRating, 
                movie_genres._c1 AS genre 
        FROM ratingsPerMovie AS rpm 
        JOIN movie_genres ON rpm.movieID = movie_genres._c0
"""
ratingPerGenre = spark.sql(query2)
ratingPerGenre.createOrReplaceTempView("ratingPerGenre")

query3 = """
        SELECT 
                rpg.genre AS Genre, 
                AVG(rpg.meanRating) AS MeanRatingPerGenre, 
                COUNT(*) AS NumberOfMoviesPerGenre 
        FROM ratingPerGenre AS rpg 
        GROUP BY rpg.genre
        ORDER BY Genre
"""

res = spark.sql(query3)
res.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")