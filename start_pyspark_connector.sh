#!/bin/bash

echo "Starting the application"
park-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true spark_connector.py --checkpoint-dir /tmp/checkpoint --bootstrap-server localhost:9092 --read-topic reddit-comments --write-topic word-counts

