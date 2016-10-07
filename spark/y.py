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

# park-submit --master local[*] --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --executor-memory 1G y.py
# spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 y.py
# yarn application -kill APPID
# SPARK_HOME_DIR/bin/spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --kill $DRIVER_ID


sc = SparkContext(appName="venturo")
ssc = StreamingContext(sc, 3)
sc.setLogLevel("WARN")    

def convertTime(ctime):
    try:
        tmp = datetime.strptime("{}".format(ctime), '%Y-%m-%d %H:%M:%S.%f')
        ctime = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        print "Time conversion failed"
    return ctime


def sanityCheck(status, ctime, city, location, driver, name=None, p1=None, p2=None):
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)

    ctime = convertTime(ctime)

    res = es.get(index='driver', doc_type='rolling', id=driver, ignore=[404, 400])

    if res['found'] and (res['_source']['status'] == status): 
        return True
    elif not res['found'] and status == 'idle': 
        doc = {"status": "idle", "ctime": ctime, "location": location, \
                   'name': name, 'city': city}
        doc = json.dumps(doc)
        q = '{{"doc": {},  "doc_as_upsert" : "true"}}'.format(doc)
        res = es.update(index='driver', doc_type='rolling', id=driver, \
                                body=q, ignore=[400, 404])
        return True
    else:
        return False

def elapsedTime(t1, ctime):
    t = datetime.strptime("{}".format(t1),'%Y-%m-%dT%H:%M:%S.%fZ')
    ctime = datetime.strptime("{}".format(ctime),'%Y-%m-%dT%H:%M:%S.%fZ') #id dis : str & unicode
    elapsed = int((ctime-t).seconds)
    return elapsed
    

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
    
    def nearby(ctime, location, driver, name, p1=None, p2=None):    
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
    
    
        res = es.search(index='passenger', doc_type='rolling', body=geo_query, ignore=[400])
        
        if res['hits']['hits']: 
            passenger = res['hits']['hits'][0]["_source"]
            doc = json.dumps({"status": "pickup", "driver": driver})
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=passenger['id'], body=q, ignore=[400,404])
            doc = {"status": "pickup", "ctime": ctime, \
                   "location": location, 'name': name, \
                   'destination':  passenger['location'], \
                   'destinationid': passenger['id'], 'id': driver}
            if not p1:
                doc['p1'] = passenger['id']
            elif not p2:
                doc['p2'] = passenger['id']
            else:
                pass  #It's full!
            doc = json.dumps(doc)
            q = '{{"doc": {}, "doc_as_upsert": "true"}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
            doc = {"status": "pickup", "ctime": ctime, "location": location, 'name': name}
            doc = json.dumps(doc)
            q = '{{"doc": {}, "doc_as_upsert": "true"}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
        else:
            isKnown = es.get(index='driver', doc_type='rolling', id=driver, ignore=[404, 400])['found']

            doc = {"ctime": ctime, "location": location, 'id': driver}
            if not isKnown: doc['status'] = "idle"
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                                body=q)
        return doc
    def isFull(driver):
        _driver = es.get(index='driver', doc_type='rolling', id=1, ignore=[400, 404])
        if _driver['found']:
            try:
                print False if _driver['_source']['p1'] and _driver['_source']['p2'] else True
            except:
                print False
        else:
            return False
        
    if sanityCheck(status, ctime, city, location, driver, name, p1=None, p2=None) and not isFull(driver):
        res = nearby(ctime, location, driver, name, p1, p2)
    else:
        res = "{'Message is not sane. Discarded'}"
    return res


def pickup(x):
    
    city = x['city']
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    status = x['status']
    ctime = convertTime(ctime)
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    def hopOn(ctime, location, driver, name, p1=None, p2=None):
 
        passenger = p2 if p2 else p1
    
        ## For demo only, so that the dots are not overlaps on the map
        newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
        newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]

        
        p = es.get(index='passenger', doc_type='rolling', id=passenger, \
                   ignore=[404, 400])['_source']
        
        if (vincenty(Point(location), Point(p['location'])).meters < 300):
            dDoc = {"status": "ontrip", "ctime": ctime, "location": location,\
                   'destination': p['destination'], 'destinationid': \
                   p['destinationid']}
            ptime = elapsedTime(p['ctime'], ctime)
            if p2:
                doc_ = json.dumps({"status": "ontrip", "ptime": ptime, \
                                   "location": newLoc_, "match": p1})
                q_ = '{{"doc": {}}}'.format(doc_)
                res = es.update(index='passenger', doc_type='rolling', \
                                id=p2, body=q_)
                doc_ = json.dumps({"status": "ontrip", "ptime": ptime, \
                                   "location": newLoc, "match": p2})
                q_ = '{{"doc": {}}}'.format(doc_)
                res = es.update(index='passenger', doc_type='rolling', \
                                id=p1, body=q_)
                
            else:
                dDoc['origin'] = location

            doc = json.dumps({"status": "ontrip", "ctime": ctime, \
                              "location": newLoc, 'ptime': ptime})
            
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=p1, body=q)
            
            
            dDoc = json.dumps(dDoc)
            dQ = '{{"doc": {}}}'.format(dDoc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                        body=dQ)
            
        else:
            doc = {"ctime": ctime, "location": location}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=driver, \
                            body=q)
            
            doc = {"ctime": ctime, "location": newLoc}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=p1, \
                            body=q)
            
            doc = {"ctime": ctime, "location": newLoc_}
            doc = json.dumps(doc)
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=p1, \
                            body=q)
        q = json.dumps({"script" : " if(! ctx._source.path.contains(path)){ ctx._source.path += path }", \
             "params" : { "path" : location }})
        #addPath = es.update(index='passenger', doc_type='rolling', id=p1, body=q)
        #if p2: addPath = es.update(index='passenger', doc_type='rolling', id=p2, body=q)
   
            
        return doc
    res = hopOn(ctime, location, driver, name, p1, p2) if \
            sanityCheck(status, ctime, city, location, driver) else '{invalid}'
        
   
    return res

def onride(x):
    city = x['city']
    ctime = x['ctime']
    location = x['location']
    driver = x['id']
    name = x['name']
    p1 = x['p1']
    p2 = x['p2']
    dest = x['destination']
    status = x['status']
    ctime = convertTime(ctime)
    
    def arrived(ctime, location, dest, driver, name, p1=None, p2=None):
        cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
        es = Elasticsearch(cluster, port=9200)
        
        isArrived = vincenty(Point(location), Point(dest)).meters < 300
        
        doc = {"status": "idle", "ctime": ctime, "location": location,\
               "destination": None, "destinationid": None, "p1": None, \
               "p2": None}

        doc = json.dumps(doc) if isArrived else json.dumps({'ctime': ctime, 'location': location})
        
        q = '{{"doc": {}}}'.format(doc)
        res = es.update(index='driver', doc_type='rolling', id=driver, \
                        body=q)
        
        newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
        newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]
        
        
        doc = {"atime": ctime, "location": newLoc}
        doc_ = {"atime": ctime, "location": newLoc_}
        
        if isArrived: 
            doc['status'] = "arrived"
            doc_['status'] = "arrived"
            doc['location'] = dest
            doc_['location'] = dest
        

        doc = json.dumps(doc)
        doc_ = json.dumps(doc_)

        q = '{{"doc": {}}}'.format(doc)
        q_ = '{{"doc": {}}}'.format(doc_)

        res = es.update(index='passenger', doc_type='rolling', id=p1, \
                        body=q)
        if p2:
            res = es.update(index='passenger', doc_type='rolling', id=p2, \
                            body=q_)
                
    if  sanityCheck(status, ctime, city, location, driver, name):
        res = arrived(ctime, location, dest, driver, name, p1, p2)
    else:
        res = '{invalid message}'
    return res
        

def updatePass(x):
    passenger = {
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
        }
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    
    #try:
    tmp = datetime.strptime("{}".format(passenger['ctime']), '%Y-%m-%d %H:%M:%S.%f')
    passenger['ctime'] = tmp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    #except:
    #    pass
    
    res = es.get(index='passenger', doc_type='rolling', id=passenger['id'], ignore=[404, 400])
    if not res['found']:
        doc = '{{"doc": {}, "doc_as_upsert" : "true"}}'.format(passenger)
        q = json.dumps(passenger)
        res = es.create(index='passenger', doc_type='rolling', id=passenger['id'], body=q)
    else:
        doc = {'ctime': passenger['ctime'], 'location': passenger['location']}
        q = '{{"doc": {}}}'.format(json.dumps(doc))
        res = es.update(index='passenger', doc_type='rolling', id=passenger['id'], body=q)
    return q
        

def main():
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    brokers = ','.join(['{}:9092'.format(i) for i in cluster])
    
    driver = KafkaUtils.createDirectStream(ssc, ['drv'], {'metadata.broker.list':brokers})
    passenger = KafkaUtils.createDirectStream(ssc, ['psg'], {'metadata.broker.list': brokers}) 
    
    P = passenger.map(lambda x: json.loads(x[1])).map(updatePass)
    D = driver.map(lambda x: json.loads(x[1]))
    idle = D.filter(lambda x: x['status']=='idle').map(assign)
    pick = D.filter(lambda x: x['status']=='pickup').map(pickup)
    secondPsg = D.filter(lambda x: x['status']=='ontrip').filter(lambda x: x['p2'] is None).map(assign)
    riding = D.filter(lambda x: x['status']=='ontrip').map(onride)

    P.pprint()
    D.pprint()
    idle.pprint()
    pick.pprint()
    secondPsg.pprint()
    riding.pprint()
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()