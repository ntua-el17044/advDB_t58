# advDB_t58

Project for the ece NTUA course Advanced Database Concepts

# Prerequisites

Version of Hadoop: hadoop-3.3.4

Version of Spark: spark-3.3.1-bin-hadoop3

Python-3.8.0

# To start the HDFS:

$start-dfs.sh

$start-yarn.sh

# To start spark master and workers respectively:

$start-master.sh

$start-worker.sh spark://192.168.0.1:7077

# To execute a query:

$python3.8 {nameofquery}

# Dataset used:

Yellow Taxi Trip Records from January 2022 to June 2022 (found at:https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

The dataset consists of 6 parquet format files, one for each month. 
Additionally we use a csv file that translates location id to Location Name (found at https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv)

All these files can be downloaded with the download.sh script

Additional information contained in the attached Report
