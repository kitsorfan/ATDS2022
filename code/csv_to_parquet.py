# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

# This script converts initial datafiles from csv to parquet format

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("csv_to_parquet").getOrCreate()

# MOVIES_GENRES
movies_genres=spark.read.csv('hdfs://master:9000/files/movie_genres.csv')
movies_genres.write.parquet('hdfs://master:9000/files/movie_genres.parquet')

# MOVIES
movies=spark.read.csv('hdfs://master:9000/files/movies.csv')
movies.write.parquet('hdfs://master:9000/files/movies.parquet')

# RATINGS
ratings=spark.read.csv('hdfs://master:9000/files/ratings.csv')
ratings.write.parquet('hdfs://master:9000/files/ratings.parquet')


# COmpile & Run from master:  spark-submit csv_to_parquet.py