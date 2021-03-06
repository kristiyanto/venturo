#################################################################
# VENTURO, a ride-sharing platform with common destinations
# Demo: http://venturo.kristiyanto.me
# Repo: http://github.com/kristiyanto/venturo
# 
# An Insight Data Engineering Project
# Silicon Valley, Autumn 2016
#
#################################################################


# This is the script to populate driver's Data
# For more detailed schema, check ../elasticsearch folder

total_drivers = 6999




#################################################################
# SETUP
#################################################################

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

#################################################################
# HELPER FUNCTIONS
#################################################################

'''
    Convert time to match with ElasticSearch format
    Input: time
    Output: time
'''

def convertTime(tm):
    try:
        t = datetime.strptime("{}".format(tm),'%Y-%m-%dT%H:%M:%S.%fZ')
        t = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        t = datetime.strptime("{}".format(tm),'%Y-%m-%d %H:%M:%S.%f')
        t = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return t
  

'''
    Load boundaries.csv
    The file store information about the lat long boundaries 
    where the driver/passenger should be placed on the map.
    Input: filename.csv
    Output: {city: [lat long], [lat long]}

'''
    
    
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


'''
    Randomly generate latlong position within given boundaries.
    Input: City
    Output: [lat, long]

'''
 
def randomLoc(id, city):
    bnd = bound[city]
    return [round(random.uniform(float(bnd[0]), float(bnd[2])),4),\
                round(random.uniform(float(bnd[1]), float(bnd[3])),4)]

'''
    Generate new lat long position to simulate driving/moving trip
    Input: (Destination [lat, long], CurrentLocation [lat, long]) 
    Output: NewLocation [lat, long]

'''


def simulateTrip(c, d):
    #q = es.get(index='driver', doc_type='rolling', id=driverID)
    #d = q['_source']['destination']
    #c = q['_source']['location']
    la = Decimal(c[0]) + (((Decimal(d[0]) - Decimal(c[0]))/step_to_dest))
    lo = Decimal(c[1]) + (((Decimal(d[1]) - Decimal(c[1]))/step_to_dest))
    return [float(str(la)), float(str(lo))]

'''
    Generate a JSON message for new driver 
    The script call ElasticSearch to check if the Driver already exsits.
    If so, it will simulate the new location based on the destination/current location
    
    Input: city
    Output: {id: , name: , ..}
'''


def generateDriver(city):
    d_id = random.randint(1, total_drivers)
    driver_mapping ={ 
            'city': city,
            'name': 'driver_{}'.format(d_id),
            'id': d_id,
            'status': 'idle',
            'location': randomLoc(d_id, city),
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
        driver_mapping = q['_source']
        driver_mapping['location'] = simulateTrip(driver_mapping['location'], driver_mapping['destination'])
    if q['found']:
        isNotLastHour =  datetime.strptime("{}".format(q['_source']['ctime']),'%Y-%m-%dT%H:%M:%S.%fZ') < datetime.now() - timedelta(hours=1)
        
        driver_mapping['ctime'] = convertTime(driver_mapping['ctime'])
        if isNotLastHour: 
            doc = '{{"doc": {}}}'.format(json.dumps(driver_mapping))
            q = es.update(index='driver', doc_type='rolling', id=d_id, ignore=[404, 400],\
                     body=doc)

    return(driver_mapping)

# Global variable for boundary file
bound = loadBoundaries(boundaries_file)

def main():
    print("Generating {} drivers".format(total_drivers))
    for n in range(total_drivers):
        city = random.choice(['CHI','NYC','CHI','CHI','NYC'])
        driver = generateDriver(city)
        u_json = json.dumps(driver).encode('utf-8')
        key = json.dumps(driver['id']).encode('utf-8')
        #print('{}'.format(driver))
        producer.send(b'drv', key, u_json)
    kafka.close()

if __name__ == "__main__":
    main()
