from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

start_time = time.time()

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def find_drama(x):
        if x[1]=="Drama":
                return [(x[0],x[1])]
        else:
                return []
def five_year(x):
        year = x[0:4]
        if "2000"<=year and year<="2004":
                return "2000-2004"
        elif "2005"<=year and year<="2009":
                return "2005-2009"
        elif "2010"<=year and year<="2014":
                return "2010-2014"
        elif "2015"<=year and year<="2019":
                return "2015-2019"

def summary_count(x):
        if (x[2] ==  '') or x[3][0:4]<"2000":
                return []
        else:
                return [(x[0],(five_year(x[3]),len(x[2].split())))]


mean_summary_per_movie = \
        sc.textFile("hdfs://master:9000/project/movies.csv"). \
        map(lambda x: split_complex(x)). \
        flatMap(lambda x: summary_count(x))

movie_drama = \
        sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
        map(lambda x: split_complex(x)). \
        flatMap(lambda x: find_drama(x))

        res = mean_summary_per_movie.join(movie_drama). \
                map(lambda x: (x[1][0][0],(x[1][0][1],1))). \
                reduceByKey(lambda x,y: (int(x[0])+int(y[0]),int(x[1])+int(y[1]))). \
                map(lambda x: (x[0],int(x[1][0])/int(x[1][1])))


        for i in res.collect():
                print(i)

        print("##############################")
        print("Elapsed time = %.2f" % (time.time()-start_time))
        print("##############################")
