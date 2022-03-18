from pyspark.sql import SparkSession
import csv
import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

start_time = time.time()

def exterior_product(x):
    out=[]
    for i in x[1][0]:
        for j in x[1][1]:
            out.append((x[0],(i,j)))
    return out

res = \
        sc.textFile("hdfs://master:9000/project/genre_small.csv,hdfs://master:9000/project/ratings.csv"). \
        map(lambda x: x.split(",")). \
        map(lambda x: (x[0],([(x[1])],[])) if len(x)==2 else (x[1],([],[(x[0],x[2],x[3])]))). \
        reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])). \
        flatMap(lambda x: exterior_product(x))

for i in res.take(10):
        print(i)

print("##############################")
print("Elapsed time = ", time.time()-start_time)
print("##############################")
~
