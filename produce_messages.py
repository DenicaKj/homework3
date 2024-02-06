import json
import time
from kafka import KafkaProducer

import random
import pandas as pd


producer = KafkaProducer(bootstrap_servers='localhost:29092', security_protocol="PLAINTEXT")

df = pd.read_csv("online.csv")

for i, row in df.iterrows():
    record = row.to_json()
    print(json.dumps(record))
    producer.send(
        topic="health_data",
        value=record.encode("utf-8")
    )
    time.sleep(random.randint(500, 2000) / 1000.0)
