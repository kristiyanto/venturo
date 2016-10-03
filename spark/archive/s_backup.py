from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os
import json
from datetime import datetime

#conf = SparkConf().setMaster("local").setAppName("Trip").set("spark.hadoop.validateOutputSpecs", "false")
#sc = SparkContext(conf = conf)
#sc = SparkContext("master", "trip")
sc = SparkContext(appName="trip")
ssc = StreamingContext(sc, 1)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#consumer = KafkaConsumer('passenger', group_id = 1)
#for message in consumer:
#    message = json.loads(message.value)
#    print "{}".format(message)
#    incoming = passenger(message)
#    incoming.store()
#    consumer.commit()
#consumer.close()



driver_mapping = {
  'mappings': {
    'rolling': {
      'properties': {
        'id': {'type': 'string'},
        'status': {'type': 'string'},
        'location': {'type': 'geo_point', 'lat_lon': 'true'},
        'ctime': {'type': 'date'},
        'p1': {'type': 'string'},
        'p2': {'type': 'string'},
        'destination': {'type': 'geo_point', 'lat_lon': 'true'},
        'destinationid':{'type': 'string'},
        'altdest1': {'type': 'geo_point', 'lat_lon': 'true'},
        'altdest2id':{'type': 'string'},                
        'altdest2': {'type': 'geo_point', 'lat_lon': 'true'},
        'altdest2id':{'type': 'string'},                
      }
    }
  }
}


pass_mapping = {
  'mappings': {
    'rolling': {
      'properties': {
        'id': {'type': 'string'},
        'status': {'type': 'string'},
        'match': {'type': 'string'},                
        'location': {'type': 'geo_point', 'lat_lon': 'true'},
        'ctime': {'type': 'date'},
        'driver': {'type': 'string'},
        'destination': {'type': 'geo_point', 'lat_lon': 'true'},
        'destinationid':{'type': 'string'},                
        'altdest1': {'type': 'geo_point', 'lat_lon': 'true'},
        'altdestid1':{'type': 'string'},                    
        'altdest2': {'type': 'geo_point', 'lat_lon': 'true'},
        'altdestid2':{'type': 'string'},                
      }
    }
  }
}


#es.indices.delete(index='driver', ignore=[400, 404])
#es.indices.delete(index='passenger', ignore=[400, 404])
#es.indices.create(index='driver', body=driver_mapping, ignore=400)
#es.indices.create(index='passenger', body=pass_mapping, ignore=400)


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
                                  "distance":      "5km",
                                  "distance_type": "plane", 
                                  "location": self.location
                            }
                          }
                        }
                      }
                    }

        nearby = []
        res = es.search(index='passenger', doc_type='rolling', body=geo_query )
        for i in (res['hits']['hits']):
            nearby.append(i['_id'])
        return(nearby)
    
    def assignPassenger(self):
        if len(self.nearbyPassengers())>0: 
            p = getPassenger(self.nearbyPassengers()[0]) 
            p.status = 'pickup'
            self.status = 'pickup'
            self.destination = p.location
            self.destinationid = 'pickup_{}'.format(p.id)
            return True
        else:
            return False

    def loadPassenger(self, p):
        if self.p1 == None: 
            self.p1 = p.id
        elif self.p2 == None:
            self.p2 = p.id
        else:
            print('Cab is full')
            return False
        p.status = 'ontrip'
        self.status = 'ontrip'
        self.destination = p.destination
        self.destinationid = p.destinationid
        p.driver = self.id
        p.update()
        self.update()
        return True

    def updateLocation(self):
        if isinstance(self, 'driver'):
            obj_ = getDriver(self.id)
        elif isinstance(self, 'passenger'):
            obj_ = getPassenger(self.id)    
        else:
            return(False)
        obj_.location = self.location
        obj_.update()

    def arrived(self, p):
        if self.location == self.location:
            p.status == 'arrived'
        if self.p2 != None: 
            p2 = getPassenger(self.p2)
            p2.status == 'arrived'
        d.p1 == None
        d.p2 == None
        d.destination == None
        d.destinationid == None
        d.status == 'idle'


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
    
def getDriverRecord(id):
    res = es.get(index='trip', doc_type='driver', id=id)['_source']
    return(driver(res))



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
def pipe(x):
    return(x)
                
def main():
    message = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': 'ec2-52-27-127-152.us-west-2.compute.amazonaws.com:9092'})
    #consumer = KafkaConsumer('passenger', group_id = 1)
    #consumer.subscribe('driver')
    #consumer.subscribe('passenger')
    #consumer = sc.textFile("hdfs://ec2-52-27-127-152.us-west-2.compute.amazonaws.com:9000/data/driver_input.txt")
    message.pprint()
    
    #driver = consumer.filter(lambda q: q.topics == 'driver')
    #message = consumer.map(lambda x: json.loads(x))
    #print(consumer)
    #y = consumer.map(lambda x: pipe(x))
    #consumer.collect()
    #consumer.saveAsTextFile('hdfs://ec2-52-27-127-152.us-west-2.compute.amazonaws.com:9000/data/sparkout.txt')

    
                     
                     
    #for message in consumer:
    #    if (message.topic == 'driver'):
    #        message = json.loads(message.value)
    #        incoming = passenger(message)
    #    else:
    #        message = json.loads(message.value)
    #        incoming = driver(message)
    #    print "{}".format(message)
        
    #    pipe(incoming)
    #    consumer.commit()
    #consumer.close()
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()



