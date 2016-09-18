from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import os
import json

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
print(es.info())

consumer = KafkaConsumer('driver', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for m in consumer:
    print m



