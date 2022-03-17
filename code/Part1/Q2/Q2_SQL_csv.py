from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("query1-sql").getOrCreate()

start_time = time.time()

if sys.argv[1] == "csv":
        ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/ratings.csv")
elif sys.argv[1] == "parquet":
        ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
else:
        print("Invalid argument")

ratings.createOrReplaceTempView("ratings")

rating_per_user = "select avg(_c2) as AVG_RATING, _c0 as USER from ratings group by _c0"
rating_per_user = spark.sql(rating_per_user)
rating_per_user.createOrReplaceTempView("rating_per_user")

all_users = "select count(*) as CNT from rating_per_user"
#all_users = spark.sql(all_users)
#all_users.createOrReplaceTempView("all_users")

sqlString = "select (count(*)/("+all_users+")*100) as USER_PERCENTAGE from rating_per_user as RU where RU.AVG_RATING>3"

res = spark.sql(sqlString)
res.show()

print("#########################################")
print("Elapsed time = %.2f" % (time.time() - start_time))
print("#########################################")
