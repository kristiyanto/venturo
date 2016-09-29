# This is the script to populate driver's Data
# {driver_id, time, curr_lat, curr_long, dest, load}

import time
import json
import random 
import csv
import random

from decimal import *
from kafka import KeyedProducer, KafkaClient
from datetime import datetime
from elasticsearch import Elasticsearch

getcontext().prec=6
boundaries_file = "boundaries.csv"
kafka = KafkaClient('localhost:9092')
producer = KeyedProducer(kafka)
city = 'NYC'
total_drivers = 150
sleep = 2
step_to_dest = random.randrange(1,2)

es = Elasticsearch(['ip-172-31-0-107', 'ip-172-31-0-100', \
                    ' ip-172-31-0-105', 'ip-172-31-0-106'], \
                   port=9200)



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

def simulateTrip(id, city):
    bnd = bound[city]
    q = es.get(index='driver', doc_type='rolling', id=id, ignore=[404, 400])
    if q['found'] and (q['_source']['status'] in ['ontrip', 'pickup']): 
        d = q['_source']['destination']
        c = q['_source']['location']
        la = Decimal(c[0]) + (((Decimal(d[0]) - Decimal(c[0]))/step_to_dest))
        lo = Decimal(c[1]) + (((Decimal(d[1]) - Decimal(c[1]))/step_to_dest))
        return [float(str(la)), float(str(lo))]
    else:
        return [round(random.uniform(float(bnd[0]), float(bnd[2])),4),\
                round(random.uniform(float(bnd[1]), float(bnd[3])),4)]
    
def generateDriver(city):
    d_id = random.randint(1, total_drivers)

    driver_mapping ={ 
            'name': 'driver_{}'.format(d_id),
            'id': d_id,
            'status': 'idle',
            'location': simulateTrip(d_id, city),
            'ctime': str(datetime.now()),
            'p1': None,
            'p2': None,
            'destination': None,
            'destinationid': None,
            'altdest1': None,
            'altdest1id': None,
            'altdest2': None,
            'altdest2id': None,
            'origin': None,
        }

    q = es.get(index='driver', doc_type='rolling', id=d_id, ignore=[404, 400])
    if q['found'] and (q['_source']['status'] in ['ontrip', 'pickup']): 
        driver_mapping['status'] = q['_source']['status']
        driver_mapping['p1'] = q['_source']['p1']
        driver_mapping['p2'] = q['_source']['p2']
        driver_mapping['destination'] = q['_source']['destination']
        driver_mapping['destinationid'] = q['_source']['destinationid']
    return(driver_mapping)

bound = loadBoundaries(boundaries_file)
for n in range(total_drivers):
    driver = generateDriver(city)
    u_json = json.dumps(driver).encode('utf-8')
    key = json.dumps(driver['id']).encode('utf-8')
    print('{}'.format(driver))
    producer.send(b'driver', key, u_json) 
    #time.sleep(sleep)
