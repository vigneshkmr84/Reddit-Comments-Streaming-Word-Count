import logging
import argparse
import shutil
import os
import spacy
from pyspark.sql.functions import lower, udf, current_timestamp, date_trunc, regexp_replace\
                    , trim, col, explode, split, desc, lit
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import nltk
from nltk.corpus import stopwords

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--read-topic"
                        , help="Kafka topic to stream messages"
                        , type=str
                        , required=True)

    parser.add_argument("--write-topic"
                        , help="Kafka topic to push messages"
                        , type=str
                        , required=True)

    parser.add_argument("--bootstrap-server"
                        , help="Kafka bootstrap server"
                        , type=str
                        , required=True)

    parser.add_argument("--checkpoint-dir"
                        , help="Checkpoint dir"
                        , type=str
                        , required=True)

    ags = parser.parse_args()

    ckpoint_dir = ags.checkpoint_dir
    if os.path.exists(ckpoint_dir):
        print('Clearing checkpoint dir : ', ckpoint_dir)
        shutil.rmtree(ckpoint_dir)

    return ags

def run(bootstrap_server, read_topic, write_topic, checkpoint_dir):
    nltk.download('stopwords')
    ner = spacy.load("en_core_web_sm")
    ner_label_list = ["PERSON","NORP","FAC","ORG","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","LAW","LANGUAGE"]
    stop_words = stopwords.words('english')

    spark = SparkSession.builder.appName("reddit-comments-stream-application").getOrCreate()

    # supress the unwanted logging
    spark.sparkContext.setLogLevel("WARN")
    logging.getLogger().setLevel(logging.ERROR)
    # logging.getLogger("pyspark.sql.streaming.kafka.KafkaDataConsumer").setLevel(logging.WARN)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # Create a DataFrame representing the stream of input lines from Kafka
    df = spark.readStream.format("kafka") \
                        .option("kafka.bootstrap.servers", bootstrap_server) \
                        .option("subscribe", read_topic) \
                        .load() \
                        .selectExpr("CAST(value AS STRING)")

    df.printSchema()

    words = df

    @udf(returnType=StringType())
    def perform_ner(comments):
        text = ner(comments)
        return ','.join([ ent.text for ent in text.ents if ent.label_ in ner_label_list])

    # Filter NER words & remove stop words
    words = words.select(perform_ner("value").alias("words"))\
                 .filter(~col("words").isin(stop_words))

    # Split NER sentance with , and remove empty / null words
    words = words.select(explode(split(words.words, ','))) \
                .filter(col("words").isNotNull() & (col("words") != "")) \
                .selectExpr("col as words")

    # make words to lower, remove punctuations and trim
    words = words.withColumn("words", lower("words"))\
                .withColumn("words", regexp_replace("words", "[^a-zA-Z0-9\\s]+", "")) \
                .withColumn("words", trim("words"))

    # remove empty & null words
    words = words.filter(col("words").isNotNull() & (col("words") != ""))

    # perform word count
    words_count = words.groupBy('words').count()

    words_count.printSchema()

    console_query = words_count.orderBy(desc("count")).writeStream\
                        .outputMode("complete")\
                        .format("console")\
                        .start()


    query = words_count\
            .selectExpr("CAST(words AS STRING) AS key",  "to_json(struct(*)) AS value") \
            .writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_server) \
            .outputMode("update") \
            .option("topic", write_topic).option("checkpointLocation", checkpoint_dir) \
            .start()

    query.awaitTermination()
    console_query.awaitTermination()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)

    logging.getLogger("pyspark.sql.streaming.kafka.KafkaDataConsumer").setLevel(logging.WARN)

    args = parse_args()
    read_topic = args.read_topic
    write_topic = args.write_topic
    bootstrap_server = args.bootstrap_server
    checkpoint_dir = args.checkpoint_dir

    run(bootstrap_server, read_topic, write_topic, checkpoint_dir)
