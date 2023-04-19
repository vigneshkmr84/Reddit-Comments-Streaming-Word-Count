#!/bin/bash

echo "Starting the application"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true spark_connector.py /tmp/checkpoint localhost:9092 reddit-comments word-counts

