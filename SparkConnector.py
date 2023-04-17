import logging
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import spacy
from pyspark.sql.functions import lower

from pyspark.sql.functions import udf
from pyspark.sql.functions import current_timestamp, date_trunc

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql.functions import to_json, struct, col

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark.sql.types import ArrayType, StructType, StructField, StringType


NER = spacy.load("en_core_web_sm")

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

words.printSchema()

# no need to do the word count here.
# wordCounts = words.groupBy('words', 'timestamp').count()# .withWatermark('timestamp', '10 minutes')

# words = words.withColumn("timestamp", current_timestamp())

# Print the output to the console
# query = words.writeStream.outputMode("append").format("console").start()

query = words\
        .selectExpr("CAST(words AS STRING) AS key",  "to_json(struct(*)) AS value") \
        .writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
        .outputMode("update") \
        .option("topic", "word-counts").option("checkpointLocation", "/tmp/checkpoint") \
        .start()


query.awaitTermination()
