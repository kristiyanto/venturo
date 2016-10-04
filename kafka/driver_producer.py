# This is the script to populate driver's Data
# {driver_id, time, curr_lat, curr_long, dest, load}
total_drivers = 100 


import time
import json
import random 
import csv
import random

from decimal import *
from kafka import KeyedProducer, KafkaClient
from datetime import datetime
from elasticsearch import Elasticsearch


'''
        Set the geographical boundaries and other variables
'''
boundaries_file = "boundaries.csv"
city = random.choice(['CHI'])

getcontext().prec=6
step_to_dest = random.randrange(1,2)

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', \
                    'ip-172-31-0-105', 'ip-172-31-0-106']

brokers = ','.join(['{}:9092'.format(i) for i in cluster])
#brokers = 'localhost:9092'
kafka = KafkaClient(brokers)
producer = KeyedProducer(kafka)
es = Elasticsearch(cluster, \
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
            'city': city,
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
        driver_mapping['destination'] = q['_source']['destination']
        driver_mapping['destinationid'] = q['_source']['destinationid']
        
        try:
            driver_mapping['p2'] = q['_source']['p2']
        except: 
            pass
    return(driver_mapping)

bound = loadBoundaries(boundaries_file)
for n in range(total_drivers):
    driver = generateDriver(city)
    u_json = json.dumps(driver).encode('utf-8')
    key = json.dumps(driver['id']).encode('utf-8')
    print('{}'.format(driver))
    producer.send(b'drv', key, u_json)
kafka.close()
