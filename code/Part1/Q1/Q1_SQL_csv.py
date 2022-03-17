# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis

from pyspark.sql import SparkSession

import time
import sys

"""
movie_genres.csv
0-movieID, 1-genre

movies.csv
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

spark = SparkSession.builder.appName("Q1_SQL_csv").getOrCreate()

startTime = time.time()

if sys.argv[1] == "csv":
        movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")
elif sys.argv[1] == "parquet":
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
        print(movies)
else:
        print("Invalid argument")

# We have a table movies
movies.registerTempTable("movies")

# And we will create a view with most profitable movie per year
# We don't check all constraints
sqlQuery2 = """
    SELECT 
        YEAR(_c3) AS Year, 
        MAX(((_c6-_c5)/_c5)*100) AS Profit 
    FROM movies 
    WHERE _c5!=0 
    GROUP BY Year
"""


max_profit_per_year = spark.sql(sqlQuery2)
# max_profit_per_year.show()
max_profit_per_year.createOrReplaceTempView("max_profit_per_year")

sqlQuery2 = """
    SELECT 
        YEAR(m._c3) AS Year,
        m._c1 AS Title, 
        ((m._c6-m._c5)/m._c5)*100 AS Profit 
    FROM 
        movies AS m, 
        max_profit_per_year AS MY 
    WHERE 
        year(m._c3)>=2000 AND 
        m._c3 IS NOT NULL AND 
        m._c5!=0 AND 
        m._c6!=0 AND 
        MY.Profit=((m._c6-m._c5)/m._c5)*100 AND 
        MY.Year=year(m._c3) 
    ORDER BY Year DESC
"""

sqlQ1 = spark.sql(sqlQuery2)
sqlQ1.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")

