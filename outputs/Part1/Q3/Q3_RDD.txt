from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

start_time = time.time()

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]


mean_rating_per_movie = \
        sc.textFile("hdfs://master:9000/project/ratings.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[1],(x[2],1))). \
        reduceByKey(lambda x,y:  (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        map(lambda x: (x[0], float(x[1][0])/int(x[1][1])))

movie_genre = \
        sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0],x[1]))

res = mean_rating_per_movie.join(movie_genre). \
        map(lambda x: (x[1][1],(x[1][0],1))). \
        reduceByKey(lambda x,y: (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        map(lambda x: (x[0],(float(x[1][0])/int(x[1][1]),x[1][1])))


for i in res.collect():
        print(i)

print("##############################")
print("Elapsed time = %.2f" % (time.time()-start_time))
print("##############################")
~
