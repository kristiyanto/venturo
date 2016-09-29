import os
import json
import ast
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from datetime import datetime
from geopy.distance import vincenty, Point


sc = SparkContext(appName="trip")
ssc = StreamingContext(sc, 3)
sc.setLogLevel("WARN")

def assign(x):
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass
    
    def nearby(ctime, location, driver, name, p1=None, p2=None):
        cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
        es = Elasticsearch(cluster, port=9200)

        geo_query = { "from" : 0, "size" : 1,
              "query": {
              "filtered": {
                "query" : {
                 "term" : {"status": "wait"}},
                "filter": {
                "geo_distance": {
                    "distance": '5km',
                    "distance_type": "plane", 
                    "location": location }}
              }}}
        
        res = es.search(index='passenger', doc_type='rolling', body=geo_query )

        if len(res['hits']['hits'])>0: 
            passenger = res['hits']['hits'][0]["_source"]
            doc = json.dumps({"status": "pickup", "driver": driver})
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=passenger['id'], body=q, ignore=[400,404])
            doc = {"status": "pickup", "ctime": ctime, \
                   "location": location, 'name': name, \
                   'destination':  passenger['location'], \
                   'destinationid': passenger['id']}
            if not p1:
                doc['p1'] = passenger['id']
            elif not p2:
                doc['p2'] = passenger['id']
            else:
                pass # It's full!
            doc = json.dumps(doc)
            q = '{{"doc": {}}}, "doc_as_upsert":True'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
        else:
            doc = {"status": "idle", "ctime": ctime, "location": location, 'name': name}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}, "doc_as_upsert":True'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
        return doc
        
    res = nearby(ctime, location, driver, name, p1, p2)
    return res


def onride(x):
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    dest = x['destination']
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass
    
    def arrived(ctime, location, dest, driver, name, p1=None, p2=None):
        cluster = 'ec2-52-27-127-152.us-west-2.compute.amazonaws.com'
        es = Elasticsearch(cluster, port=9200)
        if (vincenty(Point(location), Point(dest)).meters < 300):
            doc = {"status": "arrived", "ctime": ctime, "location": location,\
                  "destination": None, "destinationid": None, "p1": None, \
                  "p2": None}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
            
            newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
            newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]

            doc = {"status": "arrived", "ctime": ctime, "location": newLoc}
            doc_ = {"status": "arrived", "ctime": ctime, "location": newLoc_}
            
            doc = json.dumps(doc)
            doc_ = json.dumps(doc_)
            
            q = '{{"doc": {}}}'.format(doc)
            q_ = '{{"doc": {}}}'.format(doc_)
            
            res = es.update(index='passenger', doc_type='rolling', id=p1, \
                            body=q)
            if p2:
                res = es.update(index='passenger', doc_type='rolling', id=p2, \
                            body=q_)
                
                
            

        

def pickup(x):
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass
    
    def hopOn(ctime, location, driver, name, p1=None, p2=None):
        cluster = 'ec2-52-27-127-152.us-west-2.compute.amazonaws.com'
        es = Elasticsearch(cluster, port=9200)
        passenger = p2 if p2 else p1
        p = es.get(index='passenger', doc_type='rolling', id=passenger, ignore=[404, 400])['_source']
        if (vincenty(Point(location), Point(p['location'])).meters < 300):
            doc = {"status": "ontrip", "ctime": ctime, "location": location,\
                   'destination': passenger['destination'], 'destinationid': passenger['destinationid']}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                        body=q, ignore=[400,404])

            ## For the sake of demo only, so that the dots are not overlaps on the map
            newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
            newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]

            doc = json.dumps({"status": "ontrip", "ctime": ctime, "location": newLoc})
            if p2:
                doc_ = json.dumps({"status": "ontrip", "ctime": ctime, "location": newLoc_, match: p1})
                doc['match'] = p2
                q_ = '{{"doc": {}}}'.format(doc_)
                res = es.update(index='passenger', doc_type='rolling', id=p2, body=q_, ignore=[400,404])

            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=p1, body=q, ignore=[400,404])
        else:
            doc = {"ctime": ctime, "location": location}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
        return doc
    result = hopOn(ctime, location, driver, name, p1, p2)
    return result

def updatePass(x):
    
    passenger = {
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
        }
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    #passenger = json.loads(x)
    try:
        tmp = datetime.strptime("{}".format(passenger['ctime']), '%Y-%m-%d %H:%M:%S.%f')
        passenger['ctime'] = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass
    
    res = es.get(index='passenger', doc_type='rolling', id=passenger['id'], ignore=[404, 400])
    if not res['found']:
        doc = json.dumps(passenger)
        q = '{{"doc": {}}}'.format(doc)
        res = es.create(index='passenger', doc_type='rolling', id=passenger['id'], body=q, ignore=[400,404])
    else:
        doc = {'ctime': passenger['ctime'], 'location': passenger['location']}
        q = '{{"doc": {}}}'.format(json.dumps(doc))
        res = es.update(index='passenger', doc_type='rolling', id=passenger['id'], body=q, ignore=[400,404])
    return doc
        

def main():
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    brokers = ','.join(['{}:9092'.format(i) for i in cluster])
    driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list':brokers})

    passenger = KafkaUtils.createDirectStream(ssc, ['passenger'], {'metadata.broker.list': brokers})                                                       

    #driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': 'localhost:9092'})
    
    D = driver.map(lambda x: json.loads(x[1]))
    idle = D.filter(lambda x: x['status']=='idle').map(assign)
    pick = D.filter(lambda x: x['status']=='pickup').map(pickup)
    secondPsg = D.filter(lambda x: x['status']=='onride').filter(lambda x: x['p2'] is None).map(assign)
    riding = D.filter(lambda x: x['status']=='onride').map(onride)
    P = passenger.map(lambda x: json.loads(x[1])).map(updatePass)
    
    
    idle.pprint()
    pick.pprint()
    secondPsg.pprint()
    riding.pprint()
    P.pprint()
    
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()