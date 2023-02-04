from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, first
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

start = time.time()

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()

print("spark session created")

df = spark.read.parquet("hdfs://192.168.0.1:9000/data/yellow_tripdata_2022-0*.parquet")
lt = spark.read.option("header",True).csv("hdfs://192.168.0.1:9000/data/taxi+_zone_lookup.csv")

w = df.where(df.tpep_pickup_datetime.contains("2022-03"))\
.join(lt,df.DOLocationID == lt.LocationID,"inner")\
.where(col("Zone")=="Battery Park")\
.orderBy(desc(col("Tip_amount")))

w1 = w.collect()

w.show(1)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
