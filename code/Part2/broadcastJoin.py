# NTUA ECE, 9th Semester, ATDS
# Stylianos Kandylakis, Kitsos Orfanopoulos, Christos Tsoufis


# Broadcast join. Also this works exercise B3

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

spark=SparkSession.builder.appName("broadcastJoin").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN") # to reduce verbocity

referenceTable = "hdfs://master:9000/files/movie_genres_first100lines.csv"      # small     
logTable = "hdfs://master:9000/files/ratings.csv"                               # large

def splitComma(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

 
# O1. We have a (key, (tupleOfLogValues)) 
# O2
def ourJoin(key, logValue):
        joined = []
        if HashMap.value.get(key, None) == None: # if key doesn't exist return empty list
                return joined
        for referenceValue in HashMap.value.get(key):
                joined.append((key, (referenceValue, logValue)))
        return joined

#reads input file rows, makes (key, (values)) pairs, 
# makes a list of the values for each key and creates a HashMap.
# B1. split referenceTable
# B2. create tuples 
# B3. group by key
# B4 create tuple with lists
# NOTE: here referenceTable has only 2 columns
broadcastData = sc.textFile(referenceTable). \
        map(lambda row: splitComma(row)). \
        map(lambda x : (x[0], (x[1]) ) ). \
        groupByKey(). \
        map(lambda x : (x[0], list(x[1]))). \
        collectAsMap()

# Broadcast data from logTable and create HashTable
HashMap = sc.broadcast(broadcastData)

# MAIN1. Split logTable
# MAIN2. create Tuple
# MAIN3. Call ourJoin to create joined list with the initial data. 
#        key from the initial data will be used to "search" for data in the referenceTable       
# NOTE: Here we use movieID = x[1] as key, thus (x[1],(valules)) where (values) = (x[0], x[2], x[3])
main = \
        sc.textFile(logTable). \
        map(lambda row: splitComma(row)). \
        map(lambda x: ( x[1], (x[0], x[2], x[3]) )). \
        flatMap(lambda x: ourJoin(x[0],x[1]))

print(main.collect())


endTime = time.time() 

print("Total time: " + str(round(endTime-startTime,3))+" sec")

"""
BROADCAST JOIN
-----------------------------------------------------------
In most applications, the reference table R is much smaller
than the log table L, i.e. |R| ≪ |L|. Instead of moving
both R and L across the network as in the repartition-based
joins, we can simply broadcast the smaller table R, as it
avoids sorting on both tables and more importantly avoids
the network overhead for moving the larger table L.
Broadcast join is run as a map-only job. On each node, all
of R is retrieved from the DFS to avoid sending L over the
network. Each map task uses a main-memory hash table to
join a split of L with R.

In the init() function of each map task, broadcast join
checks if R is already stored in the local file system. If not,
it retrieves R from the DFS, partitions R on the join key,
and stores those partitions in the local file system. We do
this in the hope that not all partitions of R have to be loaded
in memory during the join.

Broadcast join dynamically decides whether to build the
hash table on L or R. The smaller of R and the split of L is
chosen to build the hash table, which is assumed to always fit
in memory. This is a safe assumption since a typical split is
less than 100MB. If R is smaller than the split of L, the init()
function is used to load all the partitions of R into memory
to build the hash table. Then the map function extracts the
join key from each record in L and uses it to probe the hash
table and generate the join output. On the other hand, if the
split of L is smaller than R, the join is not done in the map
function. Instead, the map function partitions L in the same
way as it partitioned R. Then in the close() function, the
corresponding partitions of R and L are joined. We avoid
loading those partitions in R if the corresponding partition
of L has no records. This optimization is useful when the
domain of the join key is large. Appendix A.4 shows the
details of the broadcast join implementation.

source: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf
"""