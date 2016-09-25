from kafka import KafkaConsumer, KafkaClient
from elasticsearch import Elasticsearch
from datetime import datetime
import json

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106']

es = Elasticsearch(cluster, port=9200)
distance = '5km'

'''
    Driver:
    1. Check if timestamp make sense
    2. Check if driver exists in DB, if doesn't exists, create
    3. If cab's capacity is not zero, look for nearby requests
        1. If passenger request matched:
            1. Change status from idle to on trip (if necessary)
            2. Re-route / Set destination to passenger
            3. Save information to ElasticSearch
        2. If no nearby passenger:
            1. Update current location
    4. If cab's capacity is zero:
        Check if current location matches with destination:
        1. If matched, empty the cab and mark to idle
            1. Update number of trip
            2. Send trip info to kafka (for archive)
        2. If not matched, update current location
'''
def pipeDriver(x):
    d = driver(x)
    if d.isKnown():
        if d.status in ['idle']:
            d.assignPassenger()
        elif d.status in ['pickup']:
            if not d.location == d.destination:
                d.update()
            else:
                p = getPassenger(d.p2) if d.p2 else getPassenger(d.p1)
                d.loadPassenger(d, p)

        elif d.status in ['ontrip']:
            if d.location == d.destination:
                arrived(d)
            elif not d.p2: 
                p = getPassenger(d.p1)
                p.location = d.location
                self.update()
                p.update()
                d.assignPassenger()
            else:
                p = getPassenger(d.p1)
                p.location = d.location
                p2 = getPassenger(d.p2)
                p2.location = d.location
                p.update()
                p2.update()
                self.update()
    else:
        d.store()
    return d.jsonFormat()

def pipePassenger(x):
    p = passenger(x)
    if not p.isKnown():
        p.store()
    return(p.jsonFormat())
    
                
def main():

    kc = KafkaClient(','.join(['{}:9092'.format(i) for i in cluster]), client_id='docker', timeout=120)
    kafka = KafkaConsumer(kc,
                          group_id='nyc',
                          enable_auto_commit=True,
                          auto_commit_interval_ms= 5000,
                          auto_offset_reset='smallest')  
    kafka.subscribe(['driver', 'passenger'])

    for message in kafka:
        m = json.loads(message.value)
        res=pipeDriver(m) if message.topic == 'driver' else pipePassenger(m)
        print res
        kafka.commit()
    kafka.close()


    
    

class driver(object):
    def __init__(self, *arg, **kwargs):
        for item in arg:
            for key in item:
                setattr(self, key, item[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        try:
            res = datetime.strptime("{}".format(unicode(self.ctime)), '%Y-%m-%d %H:%M:%S.%f')
            self.ctime = res.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            pass
    def jsonFormat(self):
        return(json.dumps(self.__dict__))
    def isKnown(self):
        res = es.get(index='driver', doc_type='rolling', id=self.id, ignore=[404, 400])
        return(res['found'])
    def store(self):
        res = es.create(index='driver', doc_type='rolling', id=self.id, body=self.jsonFormat())
        return(res['created'])
    def update(self):
        q = '{{"doc": {}}}'.format(self.jsonFormat())
        res = es.update(index='driver', doc_type='rolling', id=self.id, body=q)
        return(res['_version'])
    def nearbyPassengers(self):
        geo_query = { "from" : 0, "size" : 3,
                     "_source":{"include": [ "_id" ]},
                     "query": {
                "filtered": {
                    "query" : {
                        "term" : {"status": "wait"}
                    },
                    "filter": {
                        "geo_distance": {
                            "distance": distance,
                            "distance_type": "plane", 
                            "location": self.location }
                    }
                }
            }
                    }
        res = es.search(index='passenger', doc_type='rolling', body=geo_query )
        nearby = []
        
        for i in (res['hits']['hits']):
            nearby.append(i['_id'])
        return(nearby)
    
    def assignPassenger(self):
        if len(self.nearbyPassengers())>0: 
            p = getPassenger(self.nearbyPassengers()[0]) 
            p.status = 'pickup'
            self.status = 'pickup'
            p.driver = self.id
            self.destination = p.location
            self.destinationid = p.id
            p.update()
            self.update()
            return True
        else:
            return False

    def loadPassenger(self, p):
        if self.p1 == None: 
            self.p1 = p.id
            p.status = 'ontrip'
            self.status = 'ontrip'
            
        elif self.p2 == None:
            self.p2 = p.id
            p1 = getPassenger(self.p1)
            p1.match = p.id
            p.match = p1.id
            p1.status = 'match'
            p.status = 'match'
            self.status = 'full'
            p1.update()
        else:
            print('Cab is full')
            return False
        p.driver = self.id
        self.destination = p.destination
        self.destinationid = p.destinationid
        p.update()
        self.update()
        return True

class passenger(object):
    def __init__(self, *arg, **kwargs):
        for item in arg:
            for key in item:
                setattr(self, key, item[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        try:
            res = datetime.strptime("{}".format(unicode(self.ctime)), '%Y-%m-%d %H:%M:%S.%f')
            self.ctime = res.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            pass
    def jsonFormat(self):
        return(json.dumps(self.__dict__))
    def isKnown(self):
        res = es.get(index='passenger', doc_type='rolling', id=self.id, ignore=[404, 400])
        return(res['found'])
    def store(self):
        res = es.create(index='passenger', doc_type='rolling', id=self.id, body=self.jsonFormat())
        return(res['created'])
    def update(self):
        q = '{{"doc": {}}}'.format(self.jsonFormat())        
        res = es.update(index='passenger', doc_type='rolling', id=self.id, body=q)
        return(res['_version'])


def getPassenger(p_id):
    res = es.get(index='passenger', doc_type='rolling', id=p_id, ignore=404)
    return(passenger(res['_source'])) if res['found'] else res['found']

def getDriver(p_id):
    res = es.get(index='driver', doc_type='rolling', id=p_id, ignore=404)
    return(driver(res['_source'])) if res['found'] else res['found']
    
def sanityCheck(driver):
    if driver.isKnown():
        driverRecord = getDriverRecord(driver.id)
        return(driver.time > driverRecord.time)
    else:
        return(True)    

def updateLocation(self):
    if isinstance(self, 'driver'):
        obj_ = getDriver(self.id)
    elif isinstance(self, 'passenger'):
        obj_ = getPassenger(self.id)    
    else:
        return(False)
    obj_.location = self.location
    obj_.update()
    return(True)

def arrived(d):
    p = getPassenger(d.p1)
    d.p1 == None
    d.p2 == None
    d.destination == None
    d.destinationid == None
    d.status == 'idle'
    p.status = 'arrived'
    self.update()
    p.update()
    if d.p2: 
        p2 = getPassenger(d.p2)
        p2.status == 'arrived'
        p2.update()
    return(True)    
    
    
if __name__ == '__main__':
    main()
   
