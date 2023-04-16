## Create topic:
bin/kafka-topics.sh --create --topic reddit-comments --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic word-counts --bootstrap-server localhost:9092

## List topics:
bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list



## Get messages in topic:

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --from-beginning
