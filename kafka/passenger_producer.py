# This is the script to populate passenger data 
# {passenger_id, time, current lat, current long, touristDestination1, touristDestination2, touristDestination3} 
# for the stream. Ideally, this would come from User.
# Boundaries.csv contains information about the location boundaries for the generation.


# Once generated, the request then sent to Kafka.

import csv
import random
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime
import time
import json

### Global 
boundaries_file = "boundaries.csv"
tourist_attractions = "destinations.csv"
kafka = KafkaClient('localhost:9092')
producer = KeyedProducer(kafka)

last_uid = 0

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

def loadTattraction(tourist_attractions):
    # This chunck of code is to be replaced with Yelp API, or recommender systems
    attr = {}
    with open(tourist_attractions, 'r') as f:
        reader = csv.reader(f, delimiter = ',')
        for row in reader:
            name = row[1]
            loc = (float(row[2]), float(row[3]))
            attr.update({name:loc})
    f.close()
    return attr

def generatePassenger(city):
    global last_uid 
    bnd = bound[city]
    last_uid += 1
    curr_lat = random.uniform(float(bnd[0]), float(bnd[2]))
    curr_long = random.uniform(float(bnd[1]),float(bnd[3])) 
    att = random.sample(attract.values(),3)
    pass_mapping = {
            'id': last_uid,
            'status': 'Wait',
            'match': None,                
            'location': [curr_lat, curr_long],
            'ctime': str(datetime.now()),
            'driver': None,
            'destination': att[0],
            'alt-dest1': att[1],
            'alt-dest2': att[2],
          }
    return(pass_mapping)

#    return{'passenger':last_uid, 'curr_lat': curr_lat, 'curr_long': curr_long, 'curr_time': ,
#           'dest1': att[0], 'dest2': att[1], 'dest1': att[2]}
    

# Read the boundaries
bound = loadBoundaries(boundaries_file)
attract = loadTattraction(tourist_attractions)

# Generate users

for n in range(10):
        user = generatePassenger('NYC')
        u_json = json.dumps(user)
        id = json.dumps(user['id'])
        print(u_json)
        producer.send(b'passenger', id, u_json) 
        time.sleep(2)