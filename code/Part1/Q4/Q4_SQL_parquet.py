from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("query1-sql").getOrCreate()

start_time = time.time()

if sys.argv[1] == "csv":
        movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/movie_genres.csv")
        movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/movies.csv")
elif sys.argv[1] == "parquet":
        movie_genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
        movies = spark.read.parquet("hdfs://master:9000/project/movies.parquet")
else:
        print("Invalid argument")

movie_genres.createOrReplaceTempView("movie_genres")
movies.createOrReplaceTempView("movies")

def five_year(y):
  if 2000<= y and y<=2004:
    return "2000-2004"
  elif 2005<= y and y<=2009:
    return "2005-2009"
  elif 2010<= y and y<=2014:
    return "2010-2014"
  elif 2015<= y and y<=2019:
    return "2015-2019"

def summary_length(txt):
  return len(txt.split())

spark.udf.register("five_year", five_year)
spark.udf.register("summary_length", summary_length)

temp = "select summary_length(m._c2) as AVG_LENGTH, five_year(year(m._c3)) as Year from movies as m, movie_genres as mg where mg._c1 = 'Drama' and m._c0 = mg._c0 and m._c2 is not null and year(m._c3)>=2000"

sqlString = "select mean(T.AVG_LENGTH) as Avg_Length, T.Year as Year from ("+temp+") as T group by T.Year"

res = spark.sql(sqlString)
res.show()

print("#########################################")
print("Elapsed time = %.2f" % (time.time() - start_time))
print("#########################################")
~
