
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from datetime import datetime

import os
import json


cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
sc = SparkContext(appName="trip")
#sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 4)

es = Elasticsearch(cluster, port=9200)

def pipeDriver(x):
    from actors import driver, passenger
    d = driver(json.loads(x))
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

def pipePassenger(x):
    from actors import driver, passenger
    p = passenger(json.loads(x))
    if not p.isKnown():
        p.store()
    return(p.jsonFormat())
    
                
def main(): 
    
    driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})
    passenger = KafkaUtils.createDirectStream(ssc, ['passenger'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})         

    d = driver.map(lambda (key, value): json.loads(value))
    d.pprint()
    
    #y = driver.map(pipeDriver)
    #z = passenger.map(lambda x: (x, pipePassenger(x)))
    #t = passenger.map(lambda x: pipePassenger(x))
    #z.count()
    
    ssc.start()
    ssc.awaitTermination()
    
    




if __name__ == '__main__':
    main()



