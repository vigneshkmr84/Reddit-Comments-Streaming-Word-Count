# NER from Reddit Comments with Pyspark Streaming with Kafka and top 10 word extraction in Kibana

The objective of the project is to perform Named Entity Recognition (NER) from Reddit Comments streamed with Kafka and processed using PySpark. The NER is then pushed to another Kafka topic and top 10 NER words are displayed as bar-chart in Kibana.

The project involves setting up of Kafka, ELK, Pyspark along with code for NER processing and creating Kibana Dashboard for visualization.

# Setting up Infrastructure
## Kafka 

Download Kafka 3.4.0 from https://downloads.apache.org/kafka/3.4.0/kafka-3.4.0-src.tgz

#### Create topic:
`bin/kafka-topics.sh --create --topic reddit-comments --bootstrap-server localhost:9092`  
`bin/kafka-topics.sh --create --toic word-counts --bootstrap-server localhost:9092`

#### List topics:
`bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list`

#### Kafka Version:
`bin/kafka-topics.sh --version`


#### Get messages in topic:

`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --from-beginning`

#### Send Message via CLI:
`bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reddit-comments`

---

# ELK (Elastic-LogStash-Kibana)

Download the ELK Packages from the below URL's  

- Kibana: https://www.elastic.co/downloads/kibana
- Elastic Search: https://www.elastic.co/downloads/elasticsearch
- LogStash: https://www.elastic.co/downloads/logstash
<br><br>
### Starting ELK

#### Elastic Search

Disable SSL in `config/elasticsearch.yml`

``` config

xpack.security.enabled: false
xpack.security.http.ssl:
  enabled: false

```

**Run: `bin/elasticsearch`**

<br>

#### Kibana

`config/kibana.yml` (Make https -> http)
elasticsearch.hosts: ['http://192.168.10.21:9200']


**Run: `bin/kibana`**

<br>

#### LogStash

Create a new file `logstash.conf`

``` config
input {
    kafka {
            bootstrap_servers => "localhost:9092"
            topics => ["word-counts"]
    }
}

filter {
    json {
        source => "message"
    }
}

output {
   elasticsearch {
      hosts => ["http://localhost:9200"]
      index => "word-counts"
      workers => 1
    }
}

```

**Run: `bin/logstash -f logstash.conf`**

<br>

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
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true spark_connector.py <CHECKPOINT_DIR> <BOOTSTRAP_SERVER> <READ_TOPIC> <WRITE_TOPIC>
```

#### Sample
``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true spark_connector.py /tmp/checkpoint localhost:9092 reddit-comments word-counts
```

**NOTE: Make sure the `spark-sql-kafka` package version matches that of pyspark**


## Start Reddit Comments Producer

`comments_producer.py` will keep reading **COMMENTS** from `AskReddit` subreddit and push the messages to `reddit-comments` topic to Kafka Broker running in `localhost:9092`

``` bash
python -u comments_producer.py reddit-comments localhost:9092
```

**NOTE: Make sure you have `.env` file configured properly as below.**
``` config
user_name=
password=
client_id=
secret=
```

## Start PySpark NER Stream Processor

`spart_connector.py` will read messages from `reddit-comments` topic and filters the Named Entities from each comments and push it to `word-counts` topic.

``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true spark_connector.py
```
