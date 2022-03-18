# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis


# Standart Repartition join. Also this is the solution for exercise B3

from pyspark.sql import SparkSession
import csv
from io import StringIO
import time 


"""
movie_genres.csv
0-movieID, 1-genre

movies.csv
0-movieID, 1-title, 2-description, 3-releaseDate, 4-duration, 5-cost, 6-revenue, 7-popularity

ratings.csv
0-userID, 1-movieID, 2-rating, 3-ratingDate
"""

startTime = time.time()

spark=SparkSession.builder.appName("repartitionJoin").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity

referenceTable = "hdfs://master:9000/files/movie_genres_first100lines.csv"      # small     
logTable = "hdfs://master:9000/files/ratings.csv"                               # large

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def ourJoin(key, values):
        Rarray = []
        Larray = []
        for i in range(len(values)):
                elem = values[i]
                if(elem[0] == 'R'):
                        Rarray.append(elem[1:])
                elif(elem[0] == 'L'):
                        Larray.append(elem[1:])
        joined=[]
        for r in Rarray:
                for l in Larray:
                        joined.append((key,(r+l)))
        return joined

# split referenceTable
# create (key,('R',list(values)))
# NOTE: here x[0] is the key
referenceTableData = \
        sc.textFile(referenceTable). \
        map(lambda row: splitComma(row)). \
        map(lambda x : (x[0],[('R', x[1])] ) )


# split logTable
# create (key,('L',list(values)))
# NOTE: here x[1] is the key
logTableData = \
        sc.textFile(logTable). \
        map(lambda row: splitComma(row)). \
        map(lambda x: (x[1],[('L', (x[0], x[2], x[3]) )]))

# MAIN1. Union both dataset (many duplilcates)
# MAIN2. aggregate (key, list(values))
# MAIN3. ourJoin to create a joined list
main = logTableData.union(referenceTableData). \
        reduceByKey(lambda x,y: x + y ). \
        flatMap(lambda x: ourJoin(x[0], x[1]))


print(main.collect())

endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")


"""
STANDART REPARTITION JOIN:
-----------------------------------------------------------
This join strategy resembles a partitioned sort-merge join in the parallel RDBMS literature. 
It is also the join algorithm provided in the 
contributed join package of Hadoop (org.apache.hadoop.contrib.utils.join).

The standard repartition join can be implemented in one
MapReduce job. In the map phase, each map task works on
a split of either R or L. To identify which table an input
record is from, each map task tags the record with its 
originating table, and outputs the extracted join key and the
tagged record as a (key, value) pair. The outputs are then
partitioned, sorted and merged by the framework. All the
records for each join key are grouped together and eventually fed to a reducer.
For each join key, the reduce function
first separates and buffers the input records into two sets according to the table tag and then performs a cross-product
between records in these sets. The pseudo code of this algorithm is provided in Appendix A.1.


source: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf
"""