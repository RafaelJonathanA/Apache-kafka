from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
print("Kafka connected!")