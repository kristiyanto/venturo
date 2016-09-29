
import os
import json
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from datetime import datetime

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']

sc = SparkContext(appName="trip")
ssc = StreamingContext(sc, 3)
sc.setLogLevel("WARN")
es = Elasticsearch(cluster, port=9200)


def isKnown(id):
    res = es.get(index='driver', doc_type='rolling', id=id, ignore=[404, 400])
    return(res['found'])    

def toJson(x):
    result = json.loads(x[1])
    return (x[0], result)


def main():

    #driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})

    #passenger = KafkaUtils.createDirectStream(ssc, ['passenger'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})                                                       

    driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': 'localhost:9092'})    

    D = driver.map(toJson)
    E = D.groupByKey()
    idle = E.filter(lambda x: x == "idle")
    
    def nearbyPassenger(location, distance):
        geo_query = { "from" : 0, "size" : 1,
                     "query": {
                  "filtered": {
                    "query" : {
                        "term" : {"status": "wait"}},
                    "filter": {
                        "geo_distance": {
                            "distance": distance,
                            "distance_type": "plane", 
                            "location": location }}}}}

        res = es.search(index='passenger', doc_type='rolling', body=geo_query )
        nearby = []

        for i in (res['hits']['hits']):
            nearby.append(i['_id'])

        return(nearby)
    
    idle.pprint()
    
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()



