# osrm-adapter-batch

This repository contains the code for the micro-batch processes run to generate CSV files to be imported by OSRM. See [how OSRM handles traffic updates](https://github.com/Project-OSRM/osrm-backend/wiki/Traffic).

This project uses [mongodb spark-connector](https://docs.mongodb.com/spark-connector/master/scala-api/).

For development, it is recommended to use VSCode with `Scala Syntax (official)` and `Scala (Metals)` extensions for intellisense.

Usage: `spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 --class "OsrmAdapterBatch" mongodb://localhost:27017/database.collection file-path-in-hdfs.csv`

To download the produced file as a single csv to the local machine, use the command `hadoop fs -getmerge file-path-in-hdfs.csv ./local-file-path.csv`
