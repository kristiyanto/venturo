from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import os
import json
kafka = KafkaClient("localhost:9092")

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
consumer = KafkaConsumer(group_id = 1)

consumer.subscribe(topics='driver')
consumer.poll
for message in consumer:
    message = json.loads(message)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    consumer.commit()
consumer.close()