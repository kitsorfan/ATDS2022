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

spark = SparkSession.builder.appName("Q5_SQL_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity


startTime = time.time()


ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

ratings.createOrReplaceTempView("ratings")
movie_genres.createOrReplaceTempView("movie_genres")
movies.createOrReplaceTempView("movies")

# Create views with partial-order dependency 

# 1 (genre, (user, rating, movieID))
query1 = """
        SELECT 
                mg._c1 AS Genre, 
                r._c0 AS User, 
                r._c2 AS Rating, 
                r._c1 AS MovieID 
        FROM movie_genres AS mg 
        JOIN ratings AS r 
        ON r._c1 = mg._c0
"""
genre_rating_join = spark.sql(query1)
genre_rating_join.createOrReplaceTempView("genre_rating_join")

# 2
query2 = """
        SELECT 
                gr.Genre AS Genre, 
                gr.User AS User, 
                COUNT(*) AS User_Rating_Per_Genre, 
                MAX(gr.Rating) AS Max_Rating, 
                MIN(gr.Rating) AS Min_Rating 
        FROM genre_rating_join AS gr 
        GROUP BY gr.Genre, gr.User
"""
new_ratings = spark.sql(query2)
new_ratings.createOrReplaceTempView("new_ratings")

# 3
query3 = """
        SELECT 
                MAX(new_r.User_Rating_Per_Genre) AS max_ratings, 
                new_r.Genre AS Genre 
        FROM new_ratings AS new_r 
        GROUP BY new_r.Genre
"""
max_count_per_genre = spark.sql(query3)
max_count_per_genre.createOrReplaceTempView("max_count_per_genre")

# 4
query4 = """
        SELECT 
                FIRST(new_r.User) AS User, 
                new_r.Genre AS Genre 
        FROM 
                max_count_per_genre AS max_cg, 
                new_ratings AS new_r 
        WHERE 
                max_cg.Genre = new_r.Genre AND 
                max_cg.max_ratings = new_r.User_Rating_Per_Genre 
        GROUP BY new_r.Genre
"""
user_with_max_ratings_per_genre = spark.sql(query4)
user_with_max_ratings_per_genre.createOrReplaceTempView("user_with_max_ratings_per_genre")

# 5
query5 = """
        SELECT 
                new_r.Genre AS Genre, 
                new_r.User AS User, 
                new_r.User_Rating_Per_Genre AS Count_Ratings, 
                new_r.Max_Rating AS Max_Rating, 
                new_r.Min_Rating AS Min_Rating 
        FROM user_with_max_ratings_per_genre AS u_max 
        JOIN new_ratings AS new_r 
        ON 
                new_r.User=u_max.User AND 
                new_r.Genre=u_max.Genre
"""
half_result = spark.sql(query5)
half_result.createOrReplaceTempView("half_result")

# 6
query6 = """
        SELECT 
                gr.Genre AS Genre, 
                gr.MovieID AS MovieID 
        FROM 
                genre_rating_join AS gr 
        JOIN half_result AS T 
        ON 
                T.Genre=gr.Genre AND 
                T.User=gr.User AND T.Max_Rating=gr.Rating
"""
max_rating_movieID = spark.sql(query6)
max_rating_movieID.createOrReplaceTempView("max_rating_movieID")

# 7
query7 = """
        SELECT 
                gr.Genre AS Genre, 
                gr.MovieID AS MovieID 
        FROM genre_rating_join AS gr 
        JOIN half_result AS T 
        ON 
                T.Genre=gr.Genre AND 
                T.User=gr.User AND 
                T.Min_Rating=gr.Rating
"""

min_rating_movieID = spark.sql(query7)
min_rating_movieID.createOrReplaceTempView("min_rating_movieID")

# 8
query8 = """
        SELECT 
                max_ID.Genre AS Genre, 
                m._c7 AS Popularity, 
                m._c1 AS MovieTitle 
        FROM max_rating_movieID AS max_ID 
        JOIN movies AS m 
        ON max_ID.MovieID=m._c0
"""
movies_join_max_rating = spark.sql(query8)
movies_join_max_rating.createOrReplaceTempView("movies_join_max_rating")

# 9
query9 = """
        SELECT 
                min_ID.Genre AS Genre, 
                m._c7 AS Popularity,
                m._c1 AS MovieTitle 
        FROM min_rating_movieID AS min_ID 
        JOIN movies AS m 
        ON min_ID.MovieID = m._c0
"""
movies_join_min_rating = spark.sql(query9)
movies_join_min_rating.createOrReplaceTempView("movies_join_min_rating")

# 10
query10 = """
        SELECT 
                mjmr.Genre AS Genre, 
                MAX(mjmr.Popularity) AS Max_Popularity 
        FROM movies_join_max_rating AS mjmr 
        GROUP BY mjmr.Genre
"""
max_popularity_per_max_genre = spark.sql(query10)
max_popularity_per_max_genre.createOrReplaceTempView("max_popularity_per_max_genre")

# 11
query11 = """
        SELECT 
                mjmr.Genre AS Genre, 
                MAX(mjmr.Popularity) AS Max_Popularity 
        FROM movies_join_min_rating AS mjmr 
        GROUP BY mjmr.Genre
"""
max_popularity_per_min_genre = spark.sql(query11)
max_popularity_per_min_genre.createOrReplaceTempView("max_popularity_per_min_genre")

# 12
query12 = """
        SELECT 
                FIRST(m1.Genre) AS Genre, 
                FIRST(m1.MovieTitle) AS MovieTitle 
        FROM 
                movies_join_max_rating AS m1, 
                max_popularity_per_max_genre AS m2 
        WHERE 
                m1.Genre=m2.Genre AND 
                m1.Popularity=m2.Max_Popularity 
        GROUP BY m1.Genre
"""
max_movie_popularity_genre = spark.sql(query12)
max_movie_popularity_genre.createOrReplaceTempView("max_movie_popularity_genre")

# 13
query13 = """
        SELECT 
                FIRST(m1.Genre) AS Genre, 
                FIRST(m1.MovieTitle) AS MovieTitle 
        FROM 
                movies_join_min_rating AS m1, 
                max_popularity_per_min_genre AS m2 
        WHERE 
                m1.Genre=m2.Genre AND 
                m1.Popularity=m2.Max_Popularity 
        GROUP BY m1.Genre
"""
min_movie_popularity_genre = spark.sql(query13)
min_movie_popularity_genre.createOrReplaceTempView("min_movie_popularity_genre")

# 14
query14 = """
        SELECT 
                min_movie.Genre AS Genre, 
                min_movie.MovieTitle AS Worst_Movie, 
                max_movie.MovieTitle AS Favourite_Movie 
        FROM min_movie_popularity_genre AS min_movie 
        JOIN max_movie_popularity_genre AS max_movie 
        ON min_movie.Genre=max_movie.Genre
"""
fav_worst_movie_per_genre = spark.sql(query14)
fav_worst_movie_per_genre.createOrReplaceTempView("fav_worst_movie_per_genre")

# 15 LAST :)
query15 = """
        SELECT 
                hr.Genre AS Genre, 
                hr.User AS topRatingsUser, 
                hr.Count_Ratings AS NoOfRatings, 
                fw.Favourite_Movie AS MostFavoriteMovie, 
                hr.Max_Rating AS MostFavoriteMovieRating, 
                fw.Worst_Movie AS LeastFavoriteMovie, 
                hr.Min_Rating AS LeastFavoriteMovieRating 
        FROM half_result AS hr 
        JOIN fav_worst_movie_per_genre AS fw 
        ON fw.Genre=hr.Genre 
        ORDER BY fw.Genre ASC
"""

SQL_parquet = spark.sql(query15)
SQL_parquet.show()

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")