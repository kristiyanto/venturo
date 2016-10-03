from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import os
import json

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
consumer = KafkaConsumer('driver', group_id = 1)
for message in consumer:
    message = json.loads(message.value)
    print "{}".format(message)
    consumer.commit()
consumer.close()