from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("query1-sql").getOrCreate()

start_time = time.time()

if sys.argv[1] == "csv":
        ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/ratings.csv")
        movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/movie_genres.csv")
        movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/movies.csv")
elif sys.argv[1] == "parquet":
        ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
        movie_genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
        movies = spark.read.parquet("hdfs://master:9000/project/movies.parquet")
else:
        print("Invalid argument")

ratings.createOrReplaceTempView("ratings")
movie_genres.createOrReplaceTempView("movie_genres")
movies.createOrReplaceTempView("movies")

#1
genre_rating_join = "select mg._c1 as Genre, r._c0 as User, r._c2 as Rating, r._c1 as MovieID from movie_genres as mg join ratings as r on r._c1 = mg._c0"
genre_rating_join = spark.sql(genre_rating_join)
genre_rating_join.createOrReplaceTempView("genre_rating_join")

#2
new_ratings = "select gr.Genre as Genre, gr.User as User, count(*) as User_Rating_Per_Genre, max(gr.Rating) as Max_Rating, min(gr.Rating) as Min_Rating from genre_rating_join as gr group by gr.Genre, gr.User"
new_ratings = spark.sql(new_ratings)
new_ratings.createOrReplaceTempView("new_ratings")

#3
max_count_per_genre = "select max(new_r.User_Rating_Per_Genre) as max_ratings, new_r.Genre as Genre from new_ratings as new_r group by new_r.Genre"
max_count_per_genre = spark.sql(max_count_per_genre)
max_count_per_genre.createOrReplaceTempView("max_count_per_genre")

#4
user_with_max_ratings_per_genre = "select first(new_r.User) as User, new_r.Genre as Genre from max_count_per_genre as max_cg, new_ratings as new_r where max_cg.Genre = new_r.Genre and max_cg.max_ratings = new_r.User_Rating_Per_Genre group by new_r.Genre"
user_with_max_ratings_per_genre = spark.sql(user_with_max_ratings_per_genre)
user_with_max_ratings_per_genre.createOrReplaceTempView("user_with_max_ratings_per_genre")

#5
half_result = "select new_r.Genre as Genre, new_r.User as User, new_r.User_Rating_Per_Genre as Count_Ratings, new_r.Max_Rating as Max_Rating, new_r.Min_Rating as Min_Rating from user_with_max_ratings_per_genre as u_max join new_ratings as new_r on new_r.User=u_max.User and new_r.Genre=u_max.Genre"
half_result = spark.sql(half_result)
half_result.createOrReplaceTempView("half_result")

#6
max_rating_movieID = "select gr.Genre as Genre, gr.MovieID as MovieID from genre_rating_join as gr join half_result as T on T.Genre=gr.Genre and T.User=gr.User and T.Max_Rating=gr.Rating"
max_rating_movieID = spark.sql(max_rating_movieID)
max_rating_movieID.createOrReplaceTempView("max_rating_movieID")

#7
min_rating_movieID = "select gr.Genre as Genre, gr.MovieID as MovieID from genre_rating_join as gr join half_result as T on T.Genre=gr.Genre and T.User=gr.User and T.Min_Rating=gr.Rating"
min_rating_movieID = spark.sql(min_rating_movieID)
min_rating_movieID.createOrReplaceTempView("min_rating_movieID")

#8
movies_join_max_rating = "select max_ID.Genre as Genre, m._c7 as Popularity, m._c1 as MovieTitle from max_rating_movieID as max_ID join movies as m on max_ID.MovieID=m._c0"
movies_join_max_rating = spark.sql(movies_join_max_rating)
movies_join_max_rating.createOrReplaceTempView("movies_join_max_rating")

#9
movies_join_min_rating = "select min_ID.Genre as Genre, m._c7 as Popularity, m._c1 as MovieTitle from min_rating_movieID as min_ID join movies as m on min_ID.MovieID=m._c0"
movies_join_min_rating = spark.sql(movies_join_min_rating)
movies_join_min_rating.createOrReplaceTempView("movies_join_min_rating")

#10
max_popularity_per_max_genre = "select mjmr.Genre as Genre, max(mjmr.Popularity) as Max_Popularity from movies_join_max_rating as mjmr group by mjmr.Genre"
max_popularity_per_max_genre = spark.sql(max_popularity_per_max_genre)
max_popularity_per_max_genre.createOrReplaceTempView("max_popularity_per_max_genre")

#11
max_popularity_per_min_genre = "select mjmr.Genre as Genre, max(mjmr.Popularity) as Max_Popularity from movies_join_min_rating as mjmr group by mjmr.Genre"
max_popularity_per_min_genre = spark.sql(max_popularity_per_min_genre)
max_popularity_per_min_genre.createOrReplaceTempView("max_popularity_per_min_genre")

#12
max_movie_popularity_genre = "select first(m1.Genre) as Genre, first(m1.MovieTitle) as MovieTitle from movies_join_max_rating as m1, max_popularity_per_max_genre as m2 where m1.Genre=m2.Genre and m1.Popularity=m2.Max_Popularity group by m1.Genre"
max_movie_popularity_genre = spark.sql(max_movie_popularity_genre)
max_movie_popularity_genre.createOrReplaceTempView("max_movie_popularity_genre")

#13
min_movie_popularity_genre = "select first(m1.Genre) as Genre, first(m1.MovieTitle) as MovieTitle from movies_join_min_rating as m1, max_popularity_per_min_genre as m2 where m1.Genre=m2.Genre and m1.Popularity=m2.Max_Popularity group by m1.Genre"
min_movie_popularity_genre = spark.sql(min_movie_popularity_genre)
min_movie_popularity_genre.createOrReplaceTempView("min_movie_popularity_genre")

#14
fav_worst_movie_per_genre = "select min_movie.Genre as Genre, min_movie.MovieTitle as Worst_Movie, max_movie.MovieTitle as Favourite_Movie from min_movie_popularity_genre as min_movie join max_movie_popularity_genre as max_movie on min_movie.Genre=max_movie.Genre"
fav_worst_movie_per_genre = spark.sql(fav_worst_movie_per_genre)
fav_worst_movie_per_genre.createOrReplaceTempView("fav_worst_movie_per_genre")

#15
result = "select hr.Genre as Genre, hr.User as User, hr.Count_Ratings as Number_Of_Ratings, fw.Favourite_Movie as Favourite_Movie, hr.Max_Rating as Favourite_Rating, fw.Worst_Movie as Worst_Movie, hr.Min_Rating as Worst_Rating from half_result as hr join fav_worst_movie_per_genre as fw on fw.Genre=hr.Genre order by fw.Genre asc"

res = spark.sql(result)
res.show()

print("#########################################")
print("Elapsed time = %.2f" % (time.time() - start_time))
print("#########################################")
                                                 
