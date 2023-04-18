from kafka import KafkaProducer

def push_to_kafka(data):
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

    producer.send("reddit-comments", str.encode(data))
    producer.flush()
    producer.close()
    print("Pushed")

while True:
    push_to_kafka("this is a test Message on what Google has done for the World and the Organization is vividely considered to be the Greatest Gaint of the world")
