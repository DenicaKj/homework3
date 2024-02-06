from kafka import KafkaConsumer

bootstrap_servers = 'localhost:29092'
topic = 'health_data_predicted'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id='my_consumer_group',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

try:
	for message in consumer:
		print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
	pass
finally:
	# Close the consumer
	consumer.close()
