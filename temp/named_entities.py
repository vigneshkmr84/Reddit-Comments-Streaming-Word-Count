from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math
import string
import random

KAFKA_INPUT_TOPIC_NAME_CONS = "reddit-comments"
# KAFKA_OUTPUT_TOPIC_NAME_CONS = “outputmallstream”
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started")

    spark = SparkSession\
        .builder\
        .appName("Kafka-Topic-Read")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    comments = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)\
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    comments

