# This is the script to populate passenger data 
# {passenger_id, time, current lat, current long, touristDestination1, touristDestination2, touristDestination3} 
# for the stream. Ideally, this would come from User.
# Boundaries.csv contains information about the location boundaries for the generation.

totalPassenger = 5000
# Once generated, the request then sent to Kafka.

import csv
import random
import time
import json

from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime, timedelta
from decimal import *

city = random.choice(['CHI','NYC'])

### Global 
getcontext().prec=6
cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', \
                    'ip-172-31-0-105', 'ip-172-31-0-106']

brokers = ','.join(['{}:9092'.format(i) for i in cluster])

boundaries_file = "boundaries.csv"
tourist_attractions = "destinations.csv"
kafka = KafkaClient(brokers)
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

def loadTattraction(tourist_attractions, city):
    # This chunck of code is to be replaced with Yelp API, or recommender systems
    attr = {}
    with open(tourist_attractions, 'r') as f:
        reader = csv.reader(f, delimiter = ',')
        for row in reader:
            if row[0] == city:
                name = row[1]
                loc = [float(row[2]), float(row[3])]
                attr.update({name:loc})
    f.close()
    return attr

def retention(passanger):
    q = {
    'query': { 'term': {'status': 'arrived'} },
    'filter': {'range': { 'ctime': { 'gt': window }} 
        }
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]



def generatePassenger(city):
    global last_uid 
    last_uid = random.randint(1, totalPassenger)
    bnd = bound[city]
    
    curr_lat = round(random.uniform(float(bnd[0]), float(bnd[2])),4)
    curr_long = round(random.uniform(float(bnd[1]), float(bnd[3])),4)
    att = random.sample(attract.items(),3)
    pass_mapping = {
            'city': city,
            'name': "passenger_{}".format(last_uid),
            'id': last_uid,
            'status': 'wait',
            'match': None,                
            'location': [curr_lat, curr_long],
            'ctime': str(datetime.now()),
            'driver': None,
            'destination': att[0][1],
            'destinationid': att[0][0],
            'altdest1': att[1][1],
            'altdest1id': att[1][0],
            'altdest2': att[2][1],
            'altdest2id': att[2][0],
            'origin': [curr_lat, curr_long],
          }
    return(pass_mapping)
    
    q = es.get(index='passenger', doc_type='rolling', id=last_uid, ignore=[404, 400])
    if q['found'] and (q['_source']['status'] in ['ontrip', 'pickup']): 
        pass_mapping = q['_source']
        pass_mapping['ctime'] = str(datetime.now())
    if q['found'] and (q['_source']['status'] in ['arrived']): 
        pass_mapping = q['_source']
        t = datetime.strptime("{}".format(pass_mapping['ctime']),'%Y-%m-%dT%H:%M:%S.%fZ')
        if t < (datetime.today() - timedelta(hours = 3)):
            doc = json.dumps(pass_mapping)
            q = '{{"doc": {}}}'.format(doc)
            es.update(index='passenger', doc_type='rolling', id=last_uid, body=q)
    return(pass_mapping)


    
# Read the boundaries
bound = loadBoundaries(boundaries_file)
attract = loadTattraction(tourist_attractions, city)

# Generate users
print("Generating {} passengers...".format(totalPassenger))
for n in range(totalPassenger):
        user = generatePassenger(city)
        u_json = json.dumps(user).encode('utf-8')
        key = json.dumps(user['id']).encode('utf-8')
        print(u_json)
        producer.send(b'psg', key, u_json)
        #time.sleep(2)
        

