# advDB_t58
Project for the ece NTUA course Advanced Database Concepts

#Prerequisites

Version of Hadoop: hadoop-3.3.4
Version of Spark: spark-3.3.1-bin-hadoop3
Python-3.8.0

To start the HDFS:

$start-dfs.sh
$start-yarn.sh

To start spark master and workers respectively:

$start-master.sh
$start-worker.sh spark://192.168.0.1:7077

To execute a query:

$python3.8 {nameofquery}
