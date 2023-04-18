# NER from Reddit Comments with Pyspark Streaming with Kafka and top 10 word extraction in Kibana

The objective of the project is to perform Named Entity Recognition (NER) from Reddit Comments streamed with Kafka and processed using PySpark. The NER is then pushed to another Kafka topic and top 10 NER words are displayed as bar-chart in Kibana.

The project involves setting up of Kafka, ELK, Pyspark along with code for NER processing and creating Kibana Dashboard for visualization.

# Setting up Infrastructure
## Kafka 

### Download

#### Create topic:
`bin/kafka-topics.sh --create --topic reddit-comments --bootstrap-server localhost:9092`  
`bin/kafka-topics.sh --create --toic word-counts --bootstrap-server localhost:9092`

#### List topics:
`bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list`

#### Kafka Version:
`kafka-topics.sh --version`


#### Get messages in topic:

`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --from-beginning`

#### Send Message via CLI:
`bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reddit-comments`
---

# ELK (Elastic-LogStash-Kibana)

### Download

#### ElasticSearch Disk Space Issue
`PUT` request to `http://localhost:9200/_cluster/settings`
Body:
``` json
"transient": {
   
   "cluster.routing.allocation.disk.threshold_enabled": true,
    "cluster.routing.allocation.disk.watermark.low": "500mb",
    "cluster.routing.allocation.disk.watermark.high": "1gb",
    
    "cluster.info.update.interval": "1m"
  }
```

---

## PySpark

#### Use Anaconda (conda)

#### Pyspark setup  

`pip install pyspark==3.1.1`

# Code

##### Running the code

``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /Users/vigneshthirunavukkarasu/Downloads/Bigdata-Assignment-3/Producer/SparkConnector.py
```

**NOTE: Make sure the `spark-sql-kafka` package version matches that of pyspark**



