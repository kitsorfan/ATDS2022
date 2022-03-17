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

spark = SparkSession.builder.appName("Q4_SQL_csv").getOrCreate()

startTime = time.time()

movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")

movie_genres.createOrReplaceTempView("movie_genres")
movies.createOrReplaceTempView("movies")

def timePeriod(y):
  if 2000<= y and y<2005:
    return "2000-2004"
  elif 2005<= y and y<=2009:
    return "2005-2009"
  elif 2010<= y and y<=2014:
    return "2010-2014"
  elif 2015<= y and y<=2019:
    return "2015-2019"

def countWords(txt):
  return len(txt.split())

# user defined functions to spark
spark.udf.register("timePeriod", timePeriod)
spark.udf.register("countWords", countWords)

nestedQuery = """
  SELECT 
    countWords(m._c2) AS meanWords, 
    timePeriod(YEAR(m._c3)) AS timePeriod 
  FROM 
    movies AS m, 
    movie_genres AS mg 
  WHERE 
    mg._c1 = 'Drama' AND 
    m._c0 = mg._c0 AND 
    m._c2 IS NOT NULL AND 
    YEAR(m._c3)>=2000
"""

mainQuery = """
  SELECT 
    nq.timePeriod AS timePeriod, 
    AVG(nq.meanWords) AS meanWords 
  FROM ("""+nestedQuery+""") AS nq 
  GROUP BY nq.timePeriod
  ORDER BY nq.timePeriod
  """

SQL = spark.sql(mainQuery)
SQL.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")