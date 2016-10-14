#################################################################
# VENTURO, a ride-sharing platform with common destinations
# Demo: http://venturo.kristiyanto.me
# Repo: http://github.com/kristiyanto/venturo
# 
# An Insight Data Engineering Project
# Silicon Valley, Autumn 2016
#
#################################################################

import os
import json
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from datetime import datetime
from dateutil import parser
from geopy.distance import vincenty, Point


#################################################################
# To submit the job:
#
# park-submit --master local[*] --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --executor-memory 1G y.py
# spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 y.py
# yarn application -kill APPID
# SPARK_HOME_DIR/bin/spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --kill $DRIVER_ID
#################################################################

sc = SparkContext(appName="venturo")
ssc = StreamingContext(sc, 10)
sc.setLogLevel("WARN")    


#################################################################
# Helper Functions
#################################################################

'''
    Attempt to convert time from Kafka to Elasticsearch format.
    
    Input: time (str)
    Output: time (time)
'''

def convertTime(ctime):
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        print "Time conversion failed"
    return ctime


'''
    Define is location is close to other location.
    
    Input: [lat, long] and [lat, long]
    Output: True/False
'''

def isNearby(location, p):
    return True if (vincenty(Point(location), Point(p)).meters < 300) else False


'''
    Check if messages are invalid (e.g: mistmatch with previous record/
    record non-exist).
'''

def sanityCheck(es, status, ctime, city, location, driver, name=None, p1=None, p2=None):

    ctime = convertTime(ctime)
    res = es.get(index='driver', doc_type='rolling', id=driver, ignore=[404, 400])
    
    if status == "idle" and not res['found']:
        doc = {'status': 'idle', 'ctime': ctime, 'location': location, \
                   'name': name, 'city': city, 'destination': None, 'destinationid': None,\
                  'p1': None, 'p2': None, 'id': driver}
        
        q = json.dumps(doc)
        q = '{{"doc": {},  "doc_as_upsert" : "true"}}'.format(q)
        res = es.update(index='driver', doc_type='rolling', id=driver, \
                                body=q, ignore=[400, 404])
        return doc
    
    else:
        res = es.get(index='driver', doc_type='rolling', id=driver, ignore=[404, 400])
        if res['found'] and (res['_source']['status'] == status): 
            return res['_source']
        else:
            return False
        
        
'''
    Calculate time delta/elapsed time
    
    Input: Time A, Time B
    Output: time delta (Int)
'''
        
def elapsedTime(t1, ctime):
    t = datetime.strptime("{}".format(t1),'%Y-%m-%dT%H:%M:%S.%fZ')
    ctime = datetime.strptime("{}".format(ctime),'%Y-%m-%dT%H:%M:%S.%fZ') #id dis : str & unicode
    elapsed = int((ctime-t).seconds)
    return elapsed


'''
    Add location to passenger's path.
    
    Input: PassengerID (str), location [lat, long], elasticsearch
    Output: Success/Fail (bool)
'''
def appendPath(p, location, es):
    res = es.get(index='passenger', doc_type='rolling', id=p, ignore=[400,404])
    def calcDistance(dest, location): 
        return vincenty(Point(dest), Point(location)).miles

    if res['found']: 
        path = res['_source']['path']
        distance = calcDistance(location, path[-1]) + int(res['_source']['trip_distance'])
        if (vincenty(Point(path[-1]), Point(location)).miles < 100): 
            if location not in path: path.append(location)
            q = '{{"doc": {}}}'.format(json.dumps({'path':path, 'trip_distance': distance}))
            es.update(index='passenger', doc_type='rolling', id=p, body=q)
            return True
    
    return False

    
'''
    Getter Setters
    
    Input: passangerID (str), data (json), elasticsearch
    Output: elastic's transaction output
'''
def updatePassenger(p, data, es):            
    q = '{{"doc": {}}}'.format(json.dumps(data))
    res = es.update(index='passenger', doc_type='rolling', id=p, body=q, ignore=[409])
    return res
        
def updateDriver(d, data, es):
    q = '{{"doc": {}}}'.format(json.dumps(data))
    res = es.update(index='driver', doc_type='rolling', id=d, body=q)
    return res

def retrieveDriver(driver, es):
    _ = es.get(index='driver', doc_type='rolling', id=driver, ignore=[400, 404])
    return _['_source'] if _['found'] else False
  
def retrievePassenger(pID, es):
    p = es.get(index='passenger', doc_type='rolling', id=pID, \
                   ignore=[404, 400])
    return p['_source'] if p['found'] else False


'''
    Shift location by 11 meters for display purposes.
'''
def shiftLocation(location):
            newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
            newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]
            return (newLoc, newLoc_)

'''
   Main query to find nearby available passengers.
   
   
'''        
        
def scanPassenger(location, p1, es):
    geo_query = { "from" : 0, "size" : 1,
                 "query": {
              "filtered": {
                "query" : {
                 "term" : {"status": "wait"}},
                "filter": {
                    "geo_distance": {
                        "distance": '2km',
                        "distance_type": "plane", 
                        "location": location }}
            }},
                   "sort": [{
          "_geo_distance": {
               "location": location,
                  "order": "asc",
                   "unit": "km", 
          "distance_type": "plane" 
                  }}],}


    
    if p1: 
        p = retrievePassenger(p1, es)
        if p: 
            shoulds = []
            for i in [p['destinationid'],p['altdest1id'],p['altdest1id']]:
                shoulds.append({'match': {'destinationid': i}})
                shoulds.append({'match': {'altdest1id': i}}) 
                shoulds.append({'match': {'altdestid2': i}}) 
    
            destinations = [p['destinationid'], p['altdest1id'], p['altdest2id']]
            geo_query = {"size": 1, 
                 "query" : {
                  "bool" : {
                  "must" : { "term" : { "status" : "wait" }},
              "must_not" : { "term" : { "id" : p['id'] }},

                 "filter": {
                "geo_distance": {
                    "distance": '3km',
               "distance_type": "plane", 
                    "location": location }},

                "should" : shoulds,
              "minimum_should_match" : 1,
                             "boost" : 1.0
                    }},
                   "sort": [{
          "_geo_distance": {
               "location": location,
                  "order": "asc",
                   "unit": "km", 
          "distance_type": "plane" 
                  }}],
                        }

    
    res = es.search(index='passenger', doc_type='rolling', body=geo_query, ignore=[400])
    return res['hits']['hits'][0]["_source"] if res['hits']['hits'] else False

'''
    Given 2 passengers, pick up which destination to go.
'''

def newDestination(p1, p2, location):
    p1Dest = [(p1['destination'], p1['destinationid']), (p1['altdest1'], p1['altdest1id']), \
                  (p1['altdest2'], p1['altdest2id'])]
    p2Dest = [(p2['destination'], p2['destinationid']), (p2['altdest1'], p2['altdest1id']), \
                  (p2['altdest2'], p2['altdest2id'])]

    dest1 = set([p1['destinationid'], p1['altdest1id'], p1['altdest2id']])  
    dest2 = set([p2['destinationid'], p2['altdest1id'], p2['altdest2id']])

    def distance(dest, location): 
        return vincenty(Point(dest), Point(location)).miles

    far = float("inf")
    dest = (p2['destination'], p2['destinationid'])
    
    for i in xrange(len(p1Dest)):
        if (p1Dest[i][1] in list((dest1 & dest2))) and distance(p1Dest[i][0], location) < far:
            far = distance(p1Dest[i][0], location)
            dest = p1Dest[i]
    
    return 

'''
    Connect and store information to ElasticSearch
'''

def bulkStore(rdd):
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    
    for x in rdd:
        res = es.update(index='driver', doc_type='rolling', id=x[1], body=x[4])
    
    
def sendToES(x):
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    
    res = es.update(index='driver', doc_type='rolling', id=x[1], body=x[4])

        
#################################################################
# Main Functions
#################################################################

'''
    Given an idle or driver with only 1 passenger, scan and assign a passenger.
'''
                  
def assign(x):
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    status = x['status']
    city = x['city']
    ctime = convertTime(ctime)
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)

      
    def dispatch(ctime, d, location, driver, name, p, p1=None, p2=None):
        dDoc = {"ctime": ctime, "location": location}
        if p:
            doc = {"status": "pickup", "driver": driver, "ctime": ctime}

            p1 = d['p1']
            p2 = d['p2']
            
            if p2: return (0, '{abort}')
            
            elif p1: 
                dDoc['p2'] = p['id']
                
                doc['match'] = p1
                updatePassenger(p['id'], doc, es)
                appendPath(p['id'], location, es)

                doc['match'] = p['id']
                updatePassenger(p1, doc, es)
                appendPath(p1, location, es)
            else:
                dDoc['p1'] = p['id']
                updatePassenger(p['id'], doc, es)
                appendPath(p['id'], location, es)
                
            dDoc['status'] = "pickup"
            dDoc['destination'] = p['location']
            dDoc['destinationid'] = p['id']

        #updateDriver(driver, dDoc, es)
        
        bulk = (1, driver, p1, p2, '{{doc: {}}}'.format(json.dumps(dDoc)))
        return (bulk)
    
    res = (0, "{'Taxi is full.'}")
    
    d = sanityCheck(es, status, ctime, city, location, driver, name, p1=None, p2=None)
    if d and not p2:
        p = scanPassenger(location, p1, es)
        if p: res = dispatch(ctime, d, location, driver, name, p, p1, p2)
        else: res = (0, "{No nearby Passanger}")
            
    return res

'''
        Pickup
        Process messages from taxi's driver picking up passenger.
        If driver's location is within 300 meters from assigned passenger, 
        the passanger is added to the car. 
        
        Input: JSON format from Drivers
        Output: Modified JSON format 
        
'''

def pickup(x):
    
    city = x['city']
    ctime = convertTime(x['ctime'])
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    status = x['status']
    destid = x['destinationid']
    dest = x['destination']
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    
    def hopOn(ctime, d, location, driver, name, dest, p, p1=None, p2=None):
                     
        dDoc = {"ctime": ctime, "location": location}
        pDoc = {'ctime': ctime, 'location': location}
        
        if isNearby(location, dest):
            dDoc['status'] = "ontrip"
            dDoc['destination'] = p['destination']
            dDoc['destinationid'] = p['destinationid']
            dDoc['origin'] = p['location']
            
            pDoc['status'] = 'ontrip'
            pDoc['ptime'] = elapsedTime(p['ctime'], ctime)

            p1 = d['p1']
            p2 = d['p2']
            
            if p2:
                _ = pDoc
                _['location'] = shiftLocation(location)[0]
                updatePassenger(p1, _, es)
                appendPath(p1, location, es)
                    
                _ = pDoc
                _['location'] = shiftLocation(location)[1]
                updatePassenger(p2, _, es)
                appendPath(p2, location, es)
                
                newDest = newDestination(retrievePassenger(p1, es), retrievePassenger(p2, es), location)

                dDoc['destination'] = newDest[0]
                dDoc['destinationid'] = newDest[1]

            if p1:
                _ = pDoc
                _['location'] = shiftLocation(location)[0]
                updatePassenger(p1, _, es)
                appendPath(p1, location, es)
                
            else:
                return (0, {'Confused Driver.'})
                
        #updateDriver(driver, dDoc, es)
        bulk = (1, driver, p1, p2, '{{doc: {}}}'.format(json.dumps(dDoc)))
        return (bulk) 
    
    res = (0, '{invalid}')
    d = sanityCheck(es, status, ctime, city, location, driver)
    if d: 
        p = retrievePassenger(destid, es)
        if p: res = hopOn(ctime, d, location, driver, name, dest, p, p1, p2) 
        

    return res

'''
    For driver with passengers, decide if a passenger arrive to their desitinations.
'''


def onride(x):
    city = x['city']
    ctime = convertTime(x['ctime'])
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    dest = x['destination']
    status = x['status']
    ctime = convertTime(ctime)
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
 
    
    def arrived(ctime, d, location, dest, driver, name, p1=None, p2=None):

        isArrived = isNearby(location, dest)

        p1 = d['p1']
        p2 = d['p2']
        
        if isArrived: 
            dDoc = {"status": "arrived", "ctime": ctime, "location": location}
            doc = {"status": "arrived", "ctime": ctime, "location": shiftLocation(location)[1]}

        else:
            doc = {"ctime": ctime, "location": location}
            dDoc = doc

        updatePassenger(p1, doc, es)
        appendPath(p1, location, es)
        
        if p2: 
            updatePassenger(p2, doc, es)
            appendPath(p2, location, es)
        
        #updateDriver(driver, dDoc, es)

        return (1, driver, p1, p2, '{{doc: {}}}'.format(json.dumps(dDoc)))

    res = (0, '{invalid message}')    
    d = sanityCheck(es, status, ctime, city, location, driver, name)
    if d: res = arrived(ctime, d, location, dest, driver, name, p1, p2)

        
    return res
        
'''
    Catch and store passenger information. If already exists, do nothing.
'''
    
    
def updatePass(x):
    p = {
        'city' : x['city'],
        'ctime' : x['ctime'],
        'location' : x['location'],
        'id' : x['id'],
        'name' : x['name'],
        'destination' : x['destination'],
        'destinationid' : x['destinationid'],
        'altdest1' : x['altdest1'],
        'altdest1id' : x['altdest1id'],
        'altdest2' : x['altdest2'],
        'altdest2id' : x["altdest2id"],
        'status' : x['status'],
        'path': [x['location']],
        'trip_distance' : x['trip_distance'],
    }

    p['ctime'] = convertTime(p['ctime'])
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    
    isExist = retrievePassenger(p['id'], es)
    
    if not isExist:
        doc = json.dumps(p)
        q = '{{"doc": {},  "doc_as_upsert" : "true"}}'.format(doc)
        res = es.update(index='passenger', doc_type='rolling', id=p['id'], \
                        body=q, ignore=[400])
        return (1, p['id'], q)

    else: return(0, "{Welcome back, passenger.}")

    
    
    
#################################################################
# SPARK FUNCTIONS
#################################################################


def main():
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    brokers = ','.join(['{}:9092'.format(i) for i in cluster])
    
    driver = KafkaUtils.createDirectStream(ssc, ['drv'], {'metadata.broker.list':brokers})
    passenger = KafkaUtils.createDirectStream(ssc, ['psg'], {'metadata.broker.list': brokers}) 
    
    D = driver.map(lambda x: json.loads(x[1]))
    P = passenger.map(lambda x: json.loads(x[1])).map(updatePass)\
        .filter(lambda x: x[0]==1).count()
        
    idle = D.filter(lambda x: x['status']=='idle').map(assign)\
        .filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(bulkStore))
        
    pick = D.filter(lambda x: x['status']=='pickup').map(pickup)\
        .filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(bulkStore))
        
    secondPsg = D.filter(lambda x: x['status']=='ontrip')\
        .filter(lambda x: x['p2'] is None)\
        .map(assign).filter(lambda x: x[0]==1)\
        .foreachRDD(lambda rdd: rdd.foreachPartition(bulkStore))

    #idle = D.filter(lambda x: x['status']=='idle').map(assign)\
    #    .filter(lambda x: x[0]==1).union(secondPsg).repartition(1).foreachRDD(lambda rdd: rdd.foreachPartition(bulkStore))
        
    riding = D.filter(lambda x: x['status']=='ontrip').map(onride)\
        .filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(bulkStore))

    P.pprint()
    #D.pprint()
    #idle.pprint()
    #pick.pprint()
    #secondPsg.pprint()
    #riding.pprint()
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()