from elasticsearch import Elasticsearch
import os
import json
from datetime import datetime

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106']

es = Elasticsearch(cluster, port=9200)



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

