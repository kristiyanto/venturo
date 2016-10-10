# This is the script to populate driver's Data
# {driver_id, time, curr_lat, curr_long, dest, load}
total_drivers = 20


import time
import json
import random 
import csv
import random

from decimal import *
from kafka import KeyedProducer, KafkaClient
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch


'''
        Set the geographical boundaries and other variables
'''
boundaries_file = "boundaries.csv"

getcontext().prec=6
step_to_dest = random.randrange(1,2)

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', \
                    'ip-172-31-0-105', 'ip-172-31-0-106']

brokers = ','.join(['{}:9092'.format(i) for i in cluster])
kafka = KafkaClient(brokers)
producer = KeyedProducer(kafka)
es = Elasticsearch(cluster, port=9200)

def convertTime(tm):
    try:
        t = datetime.strptime("{}".format(tm),'%Y-%m-%dT%H:%M:%S.%fZ')
        t = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        t = datetime.strptime("{}".format(tm),'%Y-%m-%d %H:%M:%S.%f')
        t = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return t
  

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

def randomLoc(id, city):
    bnd = bound[city]
    return [round(random.uniform(float(bnd[0]), float(bnd[2])),4),\
                round(random.uniform(float(bnd[1]), float(bnd[3])),4)]

def simulateTrip(c, d):
    #q = es.get(index='driver', doc_type='rolling', id=driverID)
    #d = q['_source']['destination']
    #c = q['_source']['location']
    la = Decimal(c[0]) + (((Decimal(d[0]) - Decimal(c[0]))/step_to_dest))
    lo = Decimal(c[1]) + (((Decimal(d[1]) - Decimal(c[1]))/step_to_dest))
    return [float(str(la)), float(str(lo))]


def generateDriver(city):
    driverID = random.randint(1, total_drivers)
    driverID = 314
    driver_mapping ={ 
            'city': city,
            'name': 'driver_{}'.format(driverID),
            'id': driverID,
            'status': 'idle',
            'location': randomLoc(driverID, city),
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

    q = es.get(index='driver', doc_type='rolling', id=driverID, ignore=[404, 400])
    if q['found'] and (q['_source']['status'] in ['ontrip', 'pickup']): 
        driver_mapping = q['_source']
        driver_mapping['location'] = simulateTrip(driver_mapping['location'], driver_mapping['destination'])
    if q['found'] and (q['_source']['status'] in ['arrived']): 
        driver_mapping['ctime'] = convertTime(driver_mapping['ctime'])
        doc = json.dumps(driver_mapping)
        doc = '{{"doc": {},  "doc_as_upsert" : "true"}}'.format(doc)

        q = es.update(index='driver', doc_type='rolling', id=driverID, ignore=[404, 400],
                     body=doc)
        return False

    return(driver_mapping)

bound = loadBoundaries(boundaries_file)

def main():
    print("Generating {} drivers".format(total_drivers))
    for n in range(total_drivers):
        city = random.choice(['CHI','NYC'])
        driver = generateDriver(city)
        u_json = json.dumps(driver).encode('utf-8')
        key = json.dumps(driver['id']).encode('utf-8')
        print('{}'.format(driver))
        producer.send(b'drv', key, u_json)
    kafka.close()

if __name__ == "__main__":
    main()
