from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("query1-sql").getOrCreate()

start_time = time.time()

if sys.argv[1] == "csv":
        movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/movie_genres.csv")
        ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/ratings.csv")
elif sys.argv[1] == "parquet":
        movie_genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
        ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
else:
        print("Invalid argument")

movie_genres.createOrReplaceTempView("movie_genres")
ratings.createOrReplaceTempView("ratings")

rating_per_movie = "select avg(R._c2) as avg_rating, R._c1 as movie_id from ratings as R group by R._c1"
rating_per_movie = spark.sql(rating_per_movie)
rating_per_movie.createOrReplaceTempView("rating_per_movie")

rating_per_genre = "select rpm.avg_rating as RATING, movie_genres._c1 as GENRE from rating_per_movie as rpm join movie_genres on rpm.movie_id = movie_genres._c0"
rating_per_genre = spark.sql(rating_per_genre)
rating_per_genre.createOrReplaceTempView("rating_per_genre")

temp = "select RG.GENRE as Movie_Genre, avg(RG.RATING) as Average_Genre_Rating, count(*) as Movies_Count from rating_per_genre as RG group by RG.GENRE"
temp = spark.sql(temp)
temp.createOrReplaceTempView("temp")

#RG.RATING is not null and RG.GENRE is not null

sqlString = "select * from temp as t order by t.Movie_Genre"

res = spark.sql(sqlString)
res.show()

print("#########################################")
print("Elapsed time = %.2f" % (time.time() - start_time))
print("#########################################")
