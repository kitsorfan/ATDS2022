# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

# This script converts initial datafiles from csv to parquet format

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("csv2parquet").getOrCreate()

movies_genres=spark.read.csv('hdfs://master:9000/data/movie_genres.csv')
movies_genres.write.parquet('hdfs://master:9000/data/movie_genres.parquet')

movies=spark.read.csv('hdfs://master:9000/data/movies.csv')
movies.write.parquet('hdfs://master:9000/data/movies.parquet')

ratings=spark.read.csv('hdfs://master:9000/data/ratings.csv')
ratings.write.parquet('hdfs://master:9000/data/ratings.parquet')

