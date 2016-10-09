# This is the script to populate passenger data 
# {passenger_id, time, current lat, current long, touristDestination1, touristDestination2, touristDestination3} 
# for the stream. Ideally, this would come from User.
# Boundaries.csv contains information about the location boundaries for the generation.

totalPassenger = 15000
# Once generated, the request then sent to Kafka.

import csv
import random
import time
import json

from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

from decimal import *


### Global 
getcontext().prec=6
cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', \
                    'ip-172-31-0-105', 'ip-172-31-0-106']

brokers = ','.join(['{}:9092'.format(i) for i in cluster])

boundaries_file = "boundaries.csv"
tourist_attractions = "destinations.csv"
kafka = KafkaClient(brokers)
producer = KeyedProducer(kafka)
cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', \
                    'ip-172-31-0-105', 'ip-172-31-0-106']
es = Elasticsearch(cluster, port=9200)


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

def convertTime(ctime):
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        print "Time conversion failed"
    return ctime
  
def generatePassenger(city, ID):
    last_uid = ID
    bnd = bound[city]
    attract = loadTattraction(tourist_attractions, city)
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
            'path': [curr_lat, curr_long]
          }
   
    q = es.get(index='passenger', doc_type='rolling', id=last_uid, ignore=[404, 400])
    if not q['found']: return pass_mapping
    
    if q['_source']['status'] in ['ontrip', 'pickup']: 
        pass_mapping = q['_source']
        pass_mapping['ctime'] = str(datetime.now())
    if q['_source']['status'] in ['arrived']:
        q = es.update(index='passenger', doc_type='rolling', id=last_uid, ignore=[404, 400], body={'doc': pass_mapping, "doc_as_upsert" : "true"})
    return(pass_mapping)


    
# Read the boundaries
bound = loadBoundaries(boundaries_file)
# Generate users

def main():
    print("Generating {} passengers...".format(totalPassenger))
    for n in xrange(totalPassenger):
        city = random.choice(['CHI','NYC'])
        user = generatePassenger(city, random.randint(1,totalPassenger))
        u_json = json.dumps(user).encode('utf-8')
        key = json.dumps(user['id']).encode('utf-8')
        print(u_json)
        producer.send(b'psg', key, u_json)
        #time.sleep(2)

if __name__ == '__main__':
    main()
