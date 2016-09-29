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

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']

sc = SparkContext(appName="trip")
ssc = StreamingContext(sc, 3)
sc.setLogLevel("WARN")
es = Elasticsearch(cluster, port=9200)

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
                    "distance": '3km',
                    "distance_type": "plane", 
                    "location": location }}
              }}}
        
        res = es.search(index='passenger', doc_type='rolling', body=geo_query )

        if len(res['hits']['hits'])>0: 
            passenger = res['hits']['hits'][0]["_source"]
            doc = json.dumps({"status": "pickup", "driver": driver})
            q = '{{"doc": {}}}'.format(doc)
            res = es.update(index='passenger', doc_type='rolling', id=passenger['id'], body=q, ignore=[400,404])
            doc = {"status": "pickup", "driver": driver, "ctime": ctime, "location": location, \
                   'name': name, 'destination': passenger['location'], 'destinationid': passenger['id']}
            if not p1:
                doc['p1'] = passenger['id']
            elif not p2:
                doc['p2'] = passenger['id']
            else:
                pass # It's full!
            doc = json.dumps(doc)
            q = '{{"doc": {}}}, "doc_as_upsert":True'.format(doc)
            res = es.update(index='driver', doc_type='rolling', id=passenger['id'], \
                            body=q)
            return res
        else:
            res = 'none'
        return res
    res = nearby(ctime, location, driver, name, p1, p2)
    return res

def main():

    #driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})

    #passenger = KafkaUtils.createDirectStream(ssc, ['passenger'], {'metadata.broker.list': ','.join(['{}:9092'.format(i) for i in cluster])})                                                       

    driver = KafkaUtils.createDirectStream(ssc, ['driver'], {'metadata.broker.list': 'localhost:9092'})
    
    D = driver.map(lambda x: json.loads(x[1]))
    idle = D.filter(lambda x: x['status']=='idle').map(assign)
    #if idle.count() > 1: 
    #    IDLE = idle.map(assign)
    #    IDLE.collect()
        
    
    #IDLE.pprint()
    idle.pprint()
    
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()