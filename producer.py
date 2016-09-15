import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime
import time
kafka = KafkaClient('localhost:9092')
src = '/home/ubuntu/data/gps.csv'

def readLocation(topic):
    producer = KeyedProducer(kafka)
    with open(src) as f:
        for i in range(100):
            line = f.next().strip()
            line = line.strip('\n')
            key = line.split(',')[0]
            print(key)
            producer.send(topic, key, line) 
            time.sleep(1)
    f.close()
            
readLocation("gps")