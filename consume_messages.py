from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['health_data_predicted'])

try:
	while True:
		msg = c.poll(1.0)

		if msg is None:
			continue
		if msg.error():
			print("Consumer error: {}".format(msg.error()))
			continue

		print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
	pass
finally:
	c.close()
