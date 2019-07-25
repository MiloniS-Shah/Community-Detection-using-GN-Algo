from pyspark import SparkContext, SQLContext
from graphframes import *
import os
import sys
os.environ["PYSPARK_SUBMIT_ARGS"] = (
"--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

import time

start= time.time()
sc = SparkContext()
sqlContext = SQLContext(sc)
threshold = int(sys.argv[1])
ipfile = sys.argv[2]
file = sc.textFile(ipfile,20)
file = file.map(lambda l: l.split(',')).filter(lambda l: "user" not in l[0])
users = file.groupByKey().mapValues(set).collectAsMap()
distinct_users = file.map(lambda l:(l[0],)).distinct()


def similar(part):
    for i in part:
        for j in users:
            if i != j:
                intersect = users[i].intersection(users[j])
                if len(intersect) >= threshold:
                    yield (i,j)


get_edges = distinct_users.map(lambda l:l[0]).mapPartitions(similar)

edges = sqlContext.createDataFrame(get_edges,['src','dst'])

unique = get_edges.map(lambda l: (l[0],)).distinct()

vertices = sqlContext.createDataFrame(unique,["id"])
print(vertices.take(2))
#creating graph
g = GraphFrame(vertices,edges)
lpa = g.labelPropagation(maxIter=5)

rdd = lpa.rdd
final = rdd.map(lambda x: (x[1],x[0])).groupByKey()
final_sorted = final.map(lambda x: sorted(x[1])).collect()

outfile = sys.argv[3]
f = open(outfile, "w+")
for i in final_sorted:
    for j in i:
        if j != i[-1]:
            f.write('\''+j+'\', ')
        else:
            f.write('\''+j+'\', ')
    f.write("\n")
f.close()

end = time.time()
print("TIME===================", end-start)
