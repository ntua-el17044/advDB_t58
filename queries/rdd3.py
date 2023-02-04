
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, desc, row_number, asc, avg, max, month, dayofmonth, hour, first
import os
import sys
import time
import datetime
import csv

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

start = time.time()

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

df = spark.read.parquet("/data/yellow_tripdata_2022-0*.parquet")
lt = spark.read.option("header",True).csv("hdfs://192.168.0.1:9000/data/taxi+_zone_lookup.csv")

temp1 = lt.withColumnRenamed("Borough", "SBorough").withColumnRenamed("Zone","SZone").drop("service_zone")
temp2 = lt.withColumnRenamed("Borough", "DBorough").withColumnRenamed("Zone","DZone").drop("service_zone")

rdd = df.rdd
rddt1 = temp1.rdd.map(lambda x: (int(x[0]),x[2]))
rddt2 = temp2.rdd.map(lambda x: (int(x[0]),x[2]))

initrdd = rdd.filter(lambda x: (x[7]!=x[8]))\
.map(lambda x:(x[7], ( (x[8],(x[4],x[16])),(x[1].month,0 if x[1].day<=15 else 1) ) ))

rdd1 = initrdd.filter(lambda x: (x[1][1][0]==1))
rdd2 = initrdd.filter(lambda x: (x[1][1][0]==2))
rdd3 = initrdd.filter(lambda x: (x[1][1][0]==3))
rdd4 = initrdd.filter(lambda x: (x[1][1][0]==4))
rdd5 = initrdd.filter(lambda x: (x[1][1][0]==5))
rdd6 = initrdd.filter(lambda x: (x[1][1][0]==6))


w1 = rdd1.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

w2 = rdd2.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

w3 = rdd3.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

w4 = rdd4.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

w5 = rdd5.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

w6 = rdd6.join(rddt1)\
.map(lambda x:(x[1][0][0][0],(((x[1][0][0][1][0],x[1][0][0][1][1]),(x[1][0][1][0],x[1][0][1][1])),x[1][1])))\
.join(rddt2)\
.filter(lambda x: (x[1][0][1]!=x[1][1]))\
.map(lambda x:((x[1][0][0][1][0],x[1][0][0][1][1]),((x[1][0][0][0][0],x[1][0][0][0][1]),1)))\
.reduceByKey(lambda x,y: ((x[0][0]+y[0][0],x[0][1]+y[0][1]),x[1]+y[1]))\
.map(lambda x: (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1])))\
.sortByKey(ascending=True)\
.collect()

print(w1)
print(w2)
print(w3)
print(w4)
print(w5)
print(w6)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
