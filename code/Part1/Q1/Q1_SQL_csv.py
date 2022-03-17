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

spark = SparkSession.builder.appName("Q1_SQL_csv").getOrCreate()

startTime = time.time()


movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")

# We have a table movies
movies.registerTempTable("movies")

# And we will create a view with max profits (no data for the movies) per year
# Because of GROUP we will need an extra query (movie title can't "survive" the grouping)
sqlQuery1 = """
    SELECT 
        YEAR(_c3) AS Year, 
        MAX(((_c6-_c5)/_c5)*100) AS Profit 
    FROM movies 
    WHERE _c5!=0 
    GROUP BY Year
"""

maxProfitPerYear = spark.sql(sqlQuery1)
maxProfitPerYear.createOrReplaceTempView("maxProfitPerYear")
# Almost the same with the previous query. Checks if profit is equal to previous one, 
# if so, we have found the maximum
sqlQuery2 = """
    SELECT 
        YEAR(m._c3) AS Year,
        m._c1 AS Title, 
        ((m._c6-m._c5)/m._c5)*100 AS Profit 
    FROM 
        movies AS m, 
        maxProfitPerYear AS mppy 
    WHERE 
        year(m._c3)>=2000 AND 
        m._c3 IS NOT NULL AND 
        m._c5!=0 AND 
        m._c6!=0 AND 
        mppy.Profit=((m._c6-m._c5)/m._c5)*100 AND 
        mppy.Year=year(m._c3) 
    ORDER BY Year DESC
"""

sqlQ1 = spark.sql(sqlQuery2)
sqlQ1.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")

