import logging
import spacy
from pyspark.sql.functions import lower, udf, current_timestamp, date_trunc, regexp_replace\
                    , trim, col, explode, split, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark.sql.types import StringType
import shutil
import os
import nltk
from nltk.corpus import stopwords

# specify the checkpoint directory
checkpoint_dir = "/tmp/checkpoint"

# delete the checkpoint directory if it exists
if os.path.exists(checkpoint_dir):
    shutil.rmtree(checkpoint_dir)

nltk.download('stopwords')
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


spark = SparkSession.builder.appName("reddit-comments-stream-application").getOrCreate()

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

stop_words = stopwords.words('english')

words = words.select(perform_ner("value").alias("words"))
words = words.filter(~col("words").isin(stop_words))
words = words.select(explode(split(words.words, ',')))

# remove empty & null words
words = words.filter(col("words").isNotNull() & (col("words") != ""))
words = words.selectExpr("col as words")

# words = words.selectExpr("words as words")

words.printSchema()

words = words.withColumn("words", lower("words"))\
            .withColumn("words", regexp_replace("words", "[^a-zA-Z0-9\\s]+", "")) \
            .withColumn("words", trim("words")) \
            .withColumn("count", lit(1))

# words = words.withColumn("timestamp", date_trunc("second", current_timestamp()))
# words = words.withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
# words = words.withColumn("count", lit(1))

words.printSchema()

window_size = "5 seconds"

# need to do the word count here.
words = words.groupBy('words').count()
# wordCounts = words.groupBy('words').count()

words = words.withColumn("timestamp", date_trunc("second", current_timestamp()))

words.printSchema()

# words_tumbling = words.groupBy(window("timestamp", windowDuration=window_size), "words").count().alias("count")

# words = words.withColumn("timestamp", current_timestamp())

# Print the output to the console
# query = words.writeStream.outputMode("append").format("console").start()

# use this schema for pushing to the final topic
final_schema = StructType([
    StructField("words", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("count", IntegerType(), True)
])

console_query = words.orderBy(desc("count")).writeStream\
                    .outputMode("complete")\
                    .format("console")\
                    .start()


query = words\
        .selectExpr("CAST(words AS STRING) AS key",  "to_json(struct(*)) AS value") \
        .writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
        .outputMode("update") \
        .option("topic", "word-counts").option("checkpointLocation", checkpoint_dir) \
        .start()

query.awaitTermination()
console_query.awaitTermination()
