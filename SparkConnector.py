import logging
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import spacy
import nltk
from pyspark.sql.functions import lower
from pyspark.sql.functions import current_timestamp

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql.functions import to_json, struct, col

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkConf

# from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("MyApp").set("spark.jars",
                                           "/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/kafka-clients-3.4.0.jar",
                                           # "/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/snappy-java-1.1.8.4.jar",
                                           # "/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/lz4-java-1.8.0.jar",
                                           # "/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/zstd-jni-1.5.2-1.jar",
                                           # "/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/slf4j-api-1.7.36.jar"
                                           )
# spark = SparkSession.builder.config(conf=conf).getOrCreate()


# test code to check if a file is loaded
# input_file_path = "/Users/vigneshthirunavukkarasu/Downloads/sample.txt"
# rdd = spark.sparkContext.textFile(input_file_path)
# print(rdd.collect())

# .config(conf=conf) \
# spark = SparkSession \
#         .builder \
#         .appName("test") \
#         .master("local") \
#         .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # Create DataSet representing the stream of input lines from kafka
# lines = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "reddit-comments") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")

# print("check1")

# query = spark \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()


spark = SparkSession.builder.appName(
    "reddit-comments-stream-application").getOrCreate()

# supress the unwanted logging
spark.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Create a DataFrame representing the stream of input lines from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-comments") \
    .load().selectExpr("CAST(value AS STRING)")

df.printSchema()


# line to word split
words = df.select(explode(split(df.value, ' ')).alias('word'))

# remove special characters, trim trailng and leading white space characters, 
# convert to lowercase
words = words.withColumn("word", regexp_replace("word", "[^a-zA-Z0-9\\s]+", ""))\
            .withColumn("word", trim("word"))\
            .withColumn("word", lower("word"))

# remove empty string
words = words.filter(col("word").isNotNull() & (col("word") != ""))
# words = words.withColumn("timestamp", current_timestamp())

words.printSchema()

# perform named entities

ner = words.withColumn("word", )
# word count
wordCounts = words.groupBy('word').count()# .withWatermark('timestamp', '10 minutes')

wordCounts.printSchema()

# words = words.select("word").alias('value')
# words.printSchema()

# Print the output to the console
query = words.writeStream.outputMode("append").format("console").start()

# query = wordCounts\
#         .selectExpr("CAST(word AS STRING) AS key",  "to_json(struct(*)) AS value") \
#         .writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
#         .outputMode("complete") \
#         .option("topic", "word-counts").option("checkpointLocation", "/tmp/checkpoint") \
#         .start()


query.awaitTermination()
