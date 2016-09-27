from app import app
from flask import render_template, json
from elasticsearch import Elasticsearch

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106']
es = Elasticsearch(cluster, port=9200)
size = 200
window = 'now-5d'

@app.route('/')
@app.route('/index')
def index():
    # return "Hello, World!"
    return render_template('index.html')
@app.route('/map')
def map():
    return render_template('index.html')

@app.route('/stats')
def getstats():
    drivers = activeDrivers()
    passenger = activePass()
    
    dLatLong = []
    pLatLong = []

    if len(drivers['hits']['hits']) < 1:
        ad = 0

    else:
        ad = drivers['hits']['total']
        for i in drivers['hits']['hits']:
            dLatLong.append(i['_source']['location'])

    if len(passenger['hits']['hits']) < 1:
        ap = 0

    else:
        ap = drivers['hits']['total']
        for i in drivers['hits']['hits']:
            pLatLong.append(i['_source']['location'])




    return json.dumps({'actDrivers': ad, 'dLoc': dLatLong, 'actPass': ap, 'pLoc': pLatLong})

def activeDrivers():
    q = {
    'size' : size,
    'query' : {
        'constant_score' : {
            'filter' : {
                'range' : {
                    'ctime' : {
                        'gt'  : window
                    }
                }
            }
        }
    }
    }

    res = es.search(index='driver', doc_type='rolling', body=q, ignore=[404, 400])
    return res

def activePass():
    q = {
    'size' : size,
    'query' : {
        'constant_score' : {
            'filter' : {
                'range' : {
                    'ctime' : {
                        'gt'  : window
                    }
                }
            }
        }
    }
    }

    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res
def waitPass():

    q = {
        'query': { 'term': {'status': 'arrived'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            }
        }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res

def onRide():
    q = {
    'query': { 'term': {'status': 'ontrip'} },
    'filter': {'range': { 'ctime': { 'gt': window }} 
        }
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res


