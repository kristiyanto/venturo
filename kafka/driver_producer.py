# This is the script to populate driver's Data
# {driver_id, time, curr_lat, curr_long, dest, load}

import csv
import random
from kafka import KeyedProducer, KafkaClient
from datetime import datetime
import time
import json
import random 
from elasticsearch import Elasticsearch

boundaries_file = "boundaries.csv"
last_uid = 0 
kafka = KafkaClient('localhost:9092')
producer = KeyedProducer(kafka)
city = 'NYC'
total_drivers = 20
sleep = 2
step_to_dest = 4

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
    step = 4
    bnd = bound[city]
    q = es.get(index='driver', doc_type='rolling', id=id, ignore=[404, 400])
    if q['found'] and (q['_source']['status'] in ['ontrip', 'pickup']): 
        d = q['_source']['destination']
        c = q['_source']['location']
        return [c[0] + ((c[0] - d[0])/step_to_dest), d[1] + ((d[1] - d[1])/step_to_dest)]
    else:
        return [random.uniform(float(bnd[0]), float(bnd[2])), \
                random.uniform(float(bnd[1]),float(bnd[3]))]
        
        
def generateDriver(city):
    global last_uid 
    last_uid += 1
    d_id = random.randint(1, total_drivers)
    
    driver_mapping ={ 
            'name': 'driver_{}'.format(last_uid),
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
        }

    return(driver_mapping)

bound = loadBoundaries(boundaries_file)
for n in range(total_drivers):
    driver = generateDriver(city)
    u_json = json.dumps(driver).encode('utf-8')
    key = json.dumps(city).encode('utf-8')
    print('{}'.format(driver))
    producer.send(b'driver', key, u_json) 
    time.sleep(sleep)
