
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


#from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import os
import json
from datetime import datetime
#from actors import *
cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106']

sc = SparkContext(appName="trip")
ssc = StreamingContext(sc, 3)


sc.setLogLevel("WARN")

es = Elasticsearch(cluster, port=9200)

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

    print("Hello")

    d = driver(json.loads(x[1]))
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
    return str(d.jsonFormat())

def pipePassenger(x):
    from actors import driver, passenger
    p = passenger(json.loads(x))
    if not p.isKnown():
        p.store()
    return(p.jsonFormat())
    

def newCommand(x):
    from actors import driver, passenger
    result = str(x[1])
    result = json.loads(result)
    x = driver(result)
    return (result)
    
    
def main():
    
    #driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})

    #passenger = KafkaUtils.createDirectStream(ssc, ['passenger'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})                                                       
    
    driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': 'localhost:9092'})    

    #driver.pprint()
    #passenger.pprint()

    y = driver.map(newCommand)
    #z = passenger.map(lambda x: (x, pipePassenger(x)))
    #t = passenger.map(lambda x: pipePassenger(x))
    y.pprint(2)
    
    ssc.start()
    ssc.awaitTermination()
    
    
    #kafka = KafkaConsumer('driver', 'passeger',
    #                  group_id='nyc',
    #                  auto_commit_enable=True,
    #                  auto_commit_interval_ms=30 * 1000,
    #                  auto_offset_reset='smallest')    

    #consumer = KafkaConsumer('driver', 'passenger', group_id = 1)
    #for message in kafka:
    #    d = json.loads(message.value)
    #    #print "{}".format(d)
    #    y = pipeDriver(d)

    #    consumer.task_done()
    #consumer.close()




if __name__ == '__main__':
    main()



