from pyspark.sql import SparkSession
import sys, time

disabled = sys.argv[1]

spark = SparkSession.builder.appName('query1-sql').getOrCreate()

if disabled == "Y":
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) #https://spark.apache.org/docs/latest/configuration.html
elif disabled == 'N':
        pass
else:
        raise Exception ("This setting is not available.")

df = spark.read.format("csv")

df1 = df.load("hdfs://master:9000/files/ratings.csv")
df2 = df.load("hdfs://master:9000/files/movie_genres_first100lines.csv")

df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")

sqlString = """
        SELECT * 
        FROM (
                SELECT * 
                FROM movie_genres 
                LIMIT 100) AS g, 
                ratings AS r 
        WHERE r._c1 = g._c0
"""

t1 = time.time()

spark.sql(sqlString).collect()

t2 = time.time()

spark.sql(sqlString).explain()

print("Time with choosing join type %s is %.4f sec."%("enabled" if disabled == 'N' else "disabled", t2-t1))
