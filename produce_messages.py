import json
import time
from confluent_kafka import Producer

import random
import pandas as pd

producer_config = {
    'bootstrap.servers': 'localhost:29092',
    'security.protocol': 'PLAINTEXT',
}

producer = Producer(producer_config)

df = pd.read_csv("data/online.csv")

for i, row in df.iterrows():
    record = row.to_json()
    producer.produce(
        topic="health_data",
        value=record.encode("utf-8")
    )
    producer.flush()
    time.sleep(random.randint(500, 2000) / 1000.0)
