from kafka import KafkaConsumer

consumer = KafkaConsumer('word-counts', bootstrap_servers=['localhost:9092'])

# Continuously poll for new messages
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")

# Close the consumer connection
consumer.close()
