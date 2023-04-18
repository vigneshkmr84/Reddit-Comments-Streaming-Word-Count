import logging
import spacy
from pyspark.sql.functions import lower, udf, current_timestamp, date_trunc, regexp_replace\
                    , trim, col, explode, split, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark.sql.types import StringType
import shutil
import os

# specify the checkpoint directory
checkpoint_dir = "/tmp/checkpoint"

# delete the checkpoint directory if it exists
if os.path.exists(checkpoint_dir):
    shutil.rmtree(checkpoint_dir)


NER = spacy.load("en_core_web_sm")

# from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("MyApp").set("spark.jars",
"/Users/vigneshthirunavukkarasu/Downloads/kafka_clients/kafka-clients-3.4.0.jar")
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

words = df

# ner_schema = ArrayType(StructType([
#     StructField("text", StringType()),
#     StructField("label", StringType())
# ]))

# @udf(returnType=ner_schema)
# @udf(returnType=ArrayType(StringType()))
@udf(returnType=StringType())
def perform_ner(comments):
    text = NER(comments)
    return ','.join([ ent.text for ent in text.ents])


words = words.select(perform_ner("value").alias("words"))
words = words.select(explode(split(words.words, ',')))

# remove empty & null words
words = words.filter(col("words").isNotNull() & (col("words") != ""))
words = words.selectExpr("col as words")

# words = words.selectExpr("words as words")

words.printSchema()

words = words.withColumn("words", lower("words"))\
            .withColumn("words", regexp_replace("words", "[^a-zA-Z0-9\\s]+", "")) \
            .withColumn("words", trim("words"))

words = words.withColumn("timestamp", date_trunc("minute", current_timestamp()))
# words = words.withColumn("count", lit(1))

words.printSchema()

window_size = "5 seconds"

# need to do the word count here.
# wordCounts = words.groupBy('words', 'timestamp').count()# .withWatermark('timestamp', '1 minutes')
# wordCounts = words.groupBy('words').count()

words_tumbling = words.groupBy(window("timestamp", windowDuration=window_size), "words").count().alias("count")
                            # .withWatermark("window.end", "1 minutes")
                            

# word_counts = words_tumbling \
#                 .agg({"count": "sum"}) \
#                 .withColumnRenamed("sum(count)", "total_count")

# word_counts.show()

# words_tumbling.printSchema()

# words = words.withColumn("timestamp", current_timestamp())

# Print the output to the console
query = words_tumbling.writeStream.outputMode("complete").format("console").start()

# use this schema for pushing to the final topic
final_schema = StructType([
    StructField("words", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("count", IntegerType(), True)
])

def push_to_kafka(df):
    df.selectExpr("CAST(timestamp AS STRING) as key", "to_json(struct(*)) as value")\
      .write\
      .format("kafka")\
      .option("kafka.bootstrap.servers", "localhost:9092")\
      .option("topic", "word-counts")\
      .mode("append")\
      .save()

# query = wordCounts\
# query = words_tumbling\
#         .selectExpr("CAST(words AS STRING) AS key",  "to_json(struct(*)) AS value") \
#         .writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
#         .outputMode("append") \
#         .option("topic", "word-counts").option("checkpointLocation", checkpoint_dir) \
#         .start()

# query = words_tumbling.writeStream\
#                       .foreachBatch(push_to_kafka)\
#                       .outputMode("append")\
#                       .option("checkpointLocation", "/tmp/checkpoint")\
#                       .start()


# query.awaitTermination()
