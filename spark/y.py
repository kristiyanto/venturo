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

def isNearby(location, p):
    return True if (vincenty(Point(location), Point(p['location'])).meters < 300) else False

def sanityCheck(es, status, ctime, city, location, driver, name=None, p1=None, p2=None):
    ctime = convertTime(ctime)
    res = es.get(index='driver', doc_type='rolling', id=driver, ignore=[404, 400])
    
    if res['found'] and (res['_source']['status'] == status): 
        return True
    
    elif not res['found'] and status == 'idle':
        doc = {'status': 'idle', 'ctime': ctime, 'location': location, \
                   'name': name, 'city': city, 'destination': None, 'destinationid': None,\
                  'p1': None, 'p2': None}
        
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

def appendPath(p, location, es):
    res = es.get(index='passenger', doc_type='rolling', id=p, ignore=[400,404])
    if res['found']: 
        res = res['_source']['path']
        res.append(location)
        q = '{{"doc": {}}}'.format(json.dumps({'path':res}))
        es.update(index='passenger', doc_type='rolling', id=p, body=q)
        return True
    else:
        return False

def updatePassenger(p, data, es):            
    q = '{{doc: {}}}'.format(json.dumps(data))
    res = es.update(index='passenger', doc_type='rolling', id=p, body=q)
    return res
        
def updateDriver(d, data, es):
    q = '{{doc: {}}}'.format(json.dumps(data))
    res = es.update(index='driver', doc_type='rolling', id=d, body=q)
    return res

def retrieveDriver(driver, es):
    _ = es.get(index='driver', doc_type='rolling', id=driver, ignore=[400, 404])
    return _['_source'] if _['found'] else False
  

def scanPassenger(location, es):
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
    return res['hits']['hits'][0]["_source"] if res['hits']['hits'] else False

    
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


      
    def dispatch(ctime, location, driver, name, p1=None, p2=None):
        d = retrieveDriver(driver, es)
        
        p = scanPassenger(location, es) if d else False
        
        dDoc = {"ctime": ctime, "location": location}
        if p:
            doc = {"status": "pickup", "driver": driver, "ctime": ctime}
            updatePassenger(p['id'], doc, es)

            if d['p1']: 
                dDoc['p2'] = p['id']
            else:
                dDoc['p1'] = p['id']
            dDoc['status'] = "pickup"
            dDoc['destination'] = p['location']
            dDoc['destinationid'] = p['id']
            

        updateDriver(driver, dDoc, es)
        
        bulk = (1, '{{doc: {}}}'.format(json.dumps(dDoc)))
        return (bulk)
    
    if sanityCheck(es, status, ctime, city, location, driver, name, p1=None, p2=None):
        res = dispatch(ctime, location, driver, name, p1, p2)
    else:
        res = (0, "{'Message is not sane. Discarded'}")
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
        
        def shiftLocation(location):
            newLoc = [round(location[0] - 0.0001,4), round(location[1] - 0.0001,4)]
            newLoc_ = [round(location[0] + 0.0001,4), round(location[1] + 0.0001,4)]
            return (newLoc, newLoc_)
        
        def retrievePassenger(pID):
            p = es.get(index='passenger', doc_type='rolling', id=pID, \
                   ignore=[404, 400])
            return p['_source'] if p['found'] else False
        
        def retrieveDriver(dID):
            d = es.get(index='driver', doc_type='rolling', id=dID, \
                   ignore=[404, 400])
            return d['_source']
        
        d = retrieveDriver(driver)
        
        p = retrievePassenger(d['destinationid'])
        
        dDoc = {"ctime": ctime, "location": location}
        
        pDoc = {'id': p['id'], 'ctime': ctime, 'location': location}
        
        if isNearby(location, p):
            dDoc['status'] = "ontrip"
            dDoc['destination'] = p["destination"]
            dDoc['destinationid'] = p['destinationid']
            dDoc['origin'] = location
            pDoc['status'] = 'ontrip'
            pDoc['ptime'] = elapsedTime(p['ctime'], ctime)

            if d['p2']:
                _ = pDoc
                _['id'] = p1
                _['match'] = p2
                _['location'] = shiftLocation(location)[0]
                updatePassenger(p1, _, es)
                appendPath(d['p2'], location, es)
                    
                _ = pDoc
                _['id'] = p2
                _['match'] = p1
                _['location'] = shiftLocation(location)[1]
                updatePassenger(p2, _, es)
                appendPath(d['p1'], location, es)
                    
            else:
                _ = pDoc
                _['location'] = shiftLocation(location)[0]
                updatePassenger(p1, _, es)
                appendPath(d['p1'], location, es)
                
      

        res = updateDriver(driver, dDoc, es)
        bulk = (1, '{{doc: {}}}'.format(json.dumps(dDoc)))
        return (bulk) 

    if sanityCheck(es, status, ctime, city, location, driver):
        res = hopOn(ctime, location, driver, name, p1, p2) 

    else: 
        res = (0, '{invalid}')

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
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
 
    
    def arrived(ctime, location, dest, driver, name, p1=None, p2=None):
        isArrived = True if (vincenty(Point(location), Point(dest)).meters < 300) else False
        
        d = retrieveDriver(driver, es)
        
        if isArrived: 
            dDoc = {"status": "idle", "ctime": ctime, "location": dest,\
               "destination": None, "destinationid": None, "p1": None, \
               "p2": None}
            doc = {"status": "arrived", "ctime": ctime, "location": dest}
        else:
            doc = {"ctime": ctime, "location": location}
            dDoc = doc

        updateDriver(driver, dDoc, es)
        updatePassenger(d['p1'], doc, es)
        appendPath(d['p1'], location, es)
        if d['p2']: 
            updatePassenger(d['p2'], doc, es)
            appendPath(d['p2'], location, es)

        return (1, '{{doc: {}}}'.format(json.dumps(dDoc)))
    
    if  sanityCheck(es, status, ctime, city, location, driver, name):
        res = arrived(ctime, location, dest, driver, name, p1, p2)
    else:
        res = (0, '{invalid message}')
    return res
        

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
    }

    p['ctime'] = convertTime(p['ctime'])
    
    cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
    es = Elasticsearch(cluster, port=9200)
    if p['status'] == "wait":
        res = es.get(index='passenger', doc_type='rolling', id=p['id'], ignore=[404, 400])
        if not res['found']:
            doc = json.dumps(p)
            q = '{{"doc": {},  "doc_as_upsert" : "true"}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=p['id'], \
                                    body=q, ignore=[400, 404, 409])
            return (1, q)
    else:
        doc = {'ctime': p['ctime']}
        updatePassenger(p['id'], doc, es)
        return(0, q)


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
    #D.pprint()
    idle.pprint()
    pick.pprint()
    secondPsg.pprint()
    riding.pprint()
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()