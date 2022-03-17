from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

start_time = time.time()

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]


def myFlat(x):
        meanRating = float(x[1][0])/int(x[1][1])
        if meanRating>3.0:
                return [(1,(1,1))]
        else:
                return [(1,(0,1))]

res = \
        sc.textFile("hdfs://master:9000/project/ratings.csv"). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0],(x[2],1))). \
        reduceByKey(lambda x,y:  (float(x[0])+float(y[0]),int(x[1])+int(y[1]))). \
        flatMap(lambda x: myFlat(x)). \
        reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])). \
        map(lambda x: (x[1][0]/x[1][1])*100)


for i in res.collect():
        print(i)

print("##############################")
print("Elapsed time = %.2f" % (time.time()-start_time))
print("##############################")
~
