from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

start_time = time.time()

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def split_user_genre(x):
        user, genre = x[0].split(":")
        return (genre,(user,x[1]))

def check_genres(x):
        if x[1][1]==x[1][0][2]:
                return [(x[0],(x[1][0][0],x[1][0][1],x[1][0][2]))]
        else:
                return []

def my_max(x,y):
        if float(x[1])<float(y[1]) or (x[1]==y[1] and float(x[2])<float(y[2])):
                return y
        else:
                return x

def my_min(x,y):
        if float(x[1])>float(y[1]) or (x[1]==y[1] and float(x[2])<float(y[2])):
                return y
        else:
                return x


user_movie_rating = \
        sc.textFile("hdfs://master:9000/project/ratings.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0],(x[1],x[2])))#(user,(movie,rating))



movie_user = user_movie_rating .\
        map(lambda x: (x[1][0],x[0]))#(movie,user)

movie_genre = \
        sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0],x[1]))#(movie,genre)



user_with_max_count_per_genre = movie_user.join(movie_genre). \
        map(lambda x: (x[1][0]+":"+x[1][1],1)). \
        reduceByKey(lambda x,y: int(x)+int(y)). \
        map(lambda x: split_user_genre(x)). \
        reduceByKey(lambda x,y: x if int(x[1])>int(y[1]) else y). \
        map(lambda x: (x[1][0],(x[0],x[1][1])))#(user,(genre,count))



user_max_count_ratings = user_with_max_count_per_genre.join(user_movie_rating). \
                        map(lambda x: (x[1][1][0],(x[0],x[1][1][1],x[1][0][0])))#(movie,(user,rating,genre))



user_max_count_ratings_per_genre = user_max_count_ratings.join(movie_genre). \
                                        flatMap(lambda x: check_genres(x))#(movie,(user,rating,genre))



movie_title_popularity = \
        sc.textFile("hdfs://master:9000/project/movies.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0],(x[1],x[7])))#(movie,(title,popularity))


title_rating_popularity = user_max_count_ratings_per_genre.join(movie_title_popularity). \
                map(lambda x: (x[1][0][2],(x[1][1][0],x[1][0][1],x[1][1][1])))

max_movie_ratings = title_rating_popularity .\
                reduceByKey(lambda x,y: my_max(x,y))#(genre,max info), info = (title,rating,popularity)

min_movie_ratings = title_rating_popularity .\
                reduceByKey(lambda x,y: my_min(x,y))#(genre,min info), info = (title,rating,popularity)


genre_user_count = user_with_max_count_per_genre. \
                        map(lambda x: (x[1][0],(x[0],x[1][1])))#(genre,(user, count))


max_min_movie_ratings = max_movie_ratings.join(min_movie_ratings). \
                        map(lambda x: (x[0],(x[1][0],x[1][1])))#(genre,(max info, min info))

res = genre_user_count.join(max_min_movie_ratings). \
        map(lambda x: (x[0], (x[1][0][0],x[1][0][1],x[1][1][0][0],x[1][1][0][1],x[1][1][1][0],x[1][1][1][1]))). \
        sortByKey(ascending=True)


print("(Genre, (User, Movies Reviewed, Favorite Movie Title, Favorite Movie Rating, Worst Movie Title, Worst Movie Rating))")

for i in res.collect():
        print(i)


print("##############################")
print("Elapsed time = ", time.time()-start_time)
print("##############################")
