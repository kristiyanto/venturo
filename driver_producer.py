# This is the script to populate driver's Data
# {driver_id, time, curr_lat, curr_long, dest, load}

import csv
import random
from kafka import KafkaProducer
from datetime import datetime
import time
import json

boundaries_file = "boundaries.csv"
last_uid = 0 
#kafka = KafkaClient('localhost:9092')
#producer = KeyedProducer(kafka)
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
cab_cap = 2


def loadBoundaries(boundaries_file):
    boundaries = {}
    with open(boundaries_file, 'r') as f:
        reader = csv.reader(f, delimiter = ',')
        for row in reader:
            city = row[0]
            fence = row[1:] 
            boundaries.update({city:fence})
    f.close()
    return boundaries


def generateDriver(city):
    global last_uid 
    bnd = bound[city]
    last_uid += 1
    curr_lat = random.uniform(float(bnd[0]), float(bnd[2]))
    curr_long = random.uniform(float(bnd[1]),float(bnd[3])) 
    return({'driver':last_uid, 'lat': curr_lat, 'long': curr_long, 'ctime': str(datetime.now()),
          'p1':0,'p2':0, 'cap':cab_cap, 'status':'idle'})

bound = loadBoundaries(boundaries_file)


for n in range(10):
        driver = generateDriver('NYC')
        u_json = json.dumps(driver)
        #driver = json.dumps(driver['driver'])
        print('sending {}'.format(driver))
        #producer.send(b'driver', driver, u_json) 
        producer.send(b'driver', driver) 
        time.sleep(2)
