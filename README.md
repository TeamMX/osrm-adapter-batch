# osrm-adapter-batch

This repository contains the code for the micro-batch processes run to generate CSV files to be imported by OSRM. See [how OSRM handles traffic updates](https://github.com/Project-OSRM/osrm-backend/wiki/Traffic).

This project uses [mongodb spark-connector](https://docs.mongodb.com/spark-connector/master/scala-api/).

For development, it is recommended to use VSCode with `Scala Syntax (official)` and `Scala (Metals)` extensions for intellisense.

Usage: `OsrmAdapterBatch <from mongo collection uri> <to csv path>`
