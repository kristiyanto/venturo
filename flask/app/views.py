from app import app
from flask import render_template, json
from elasticsearch import Elasticsearch

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
es = Elasticsearch(cluster, port=9200)
size = 300
window = 'now-2h'

@app.route('/')
@app.route('/index')
@app.route('/index.html')
def index():
    return render_template('index.html')
@app.route('/map')
def map():
    return render_template('index.html')

@app.route('/stats')
def getstats():
    drivers = activeDrivers()
    passenger = activePass()
    onWait = waitPass()
    onRide = ridingPass()
    match = matches()
    arr = arrived()
    idle = idleDrivers()
    avg_wait = avgWait()
    msg = matchMsg()[0]
    msgLoc = matchMsg()[1]
    
    amsg = arrivedMsg()
    if len(amsg['hits']['hits']) > 0:
        amsg = amsg['hits']['hits'][0]['_source']
        amsg = ["Driver {} just arrived to {}".format(\
            amsg['id'], amsg['destinationid']), amsg['destination']]
    else:
        amsg = ["Nothing happens", [0,0]]
    
    arrMsg = amsg[0]
    arrMsgLoc = amsg[1]

    
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
        ap = passenger['hits']['total']
        for i in passenger['hits']['hits']:
            pLatLong.append(i['_source']['location'])

    return json.dumps({'actDrivers': ad, 'match': match, 'msg': msg ,'mLoc': msgLoc, 'arrived': arr, \
                       'arrMsg': arrMsg, 'arrMsgLoc': arrMsgLoc ,'avgWait': avg_wait, 'idle': idle, \
                        'actPass': ap, \
                       'onWait': onWait, 'onRide': onRide})

@app.route('/passenger')
def P():
    res = json.dumps({'pass': getPassengers()})
    return (res)

@app.route('/driver')
def D():
    return (json.dumps({'drv':getDrivers()}))

@app.route('/path/<path:path>')
def getPath(path):
    res = es.get(index='passenger', doc_type='rolling', id=path, ignore=[400,404])
    if res['found']: 
        res = res['_source']['path']
        #return path
        return (json.dumps({'path': res}))
    else:
        return json.dumps({'path': []})


@app.route('/distance')
def distance():
    q = {'size': 10,
        'query': { 'term': {'status': 'arrived'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            },
         "sort": [{"ctime": {"order": "desc"}}]
        }
    res = []
    q = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    if q['hits']: 
        res = [ {'id': i['id'], 'trip_distance': i['trip_distance'], 'name': i['name'], 'ptime': i['ptime'] } for i in [i['_source'] for i in q['hits']['hits']]]
        res = json.dumps(res)
    return res

    
@app.route('/lines')
def lines():
    res = arrivedMsg()
    if len(res['hits']['hits']) > 0:
        res = res['hits']['hits'][0]['_source']
    else:
        res = []
    return json.dumps(res)


def getDrivers():
    drivers = activeDrivers()
    dLatLong = []

    if len(drivers['hits']['hits']) < 1:
        ad = 0

    else:
        ad = drivers['hits']['total']
        for i in drivers['hits']['hits']:
            dLatLong.append(i['_source']['location'])
    return dLatLong

def getPassengers():
    passenger = activePass()
    pLatLong = []
    
    if len(passenger['hits']['hits']) < 1:
        ap = 0

    else:
        ap = passenger['hits']['total']
        for i in passenger['hits']['hits']:
            pLatLong.append(i['_source']['location'])
            
    return pLatLong
    


def activeDrivers():
    q = {"size": size, "filter": {"range": { "ctime": { "gt": window }}}}

    res = es.search(index='driver', doc_type='rolling', body=q, ignore=[404, 400])
    return res


def activePass():
    q = {"size": size, "filter": {"range": { "ctime": { "gt": window }}}}

    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res

def idleDrivers():
    q = {'size':0,
        'query': { 'term': {'status': 'idle'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            }
        }
    res = es.search(index='driver', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]


def waitPass():
    q = {'size':0,
        'query': { 'term': {'status': 'wait'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            }
        }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]

def ridingPass():
    q = {'size':0,
    'query': { 'terms': {'status': ['ontrip']} },
    'filter': {'range': { 'ctime': { 'gt': window }} 
        }
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]

def arrived():
    q = {'size':0,
    'query': { 'term': {'status': 'arrived'} },
    'filter': {'range': { 'ctime': { 'gt': window }} 
        }
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]


def matches():

    q = { 'size': 1,
    'query' : {
        'constant_score' : {
            'query': {'exists' : { 'field' : 'match' }},
            'filter' : {
                'range': { 'ctime': { 'gt': window }}
            }
        }
    }}
    
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    
    return res['hits']['total']

def avgWait():
    q = { 'size': 0,
        "aggs" : {
            "avg_wait" : { "filter" : {  "range" : { "ctime" : { "gt" : window,}}},
            "aggs" : {
                "avg_wait" : { "avg" : { "field" : "ptime" } }
                }}}}

    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    if res['aggregations']['avg_wait']['avg_wait']['value']:
        res = round(float(res['aggregations']['avg_wait']['avg_wait']['value'])/60, 2)
    else:
        res = "N/A"
    return res



def matchMsg():
    q = { 'size': 1,
    'query' : {
        'constant_score' : {
            'query': {'exists' : { 'field' : 'match' }},
            'filter' : {
                'range': { 'ctime': { 'gt': window }}
            }
        }
    },
    "sort": [
    {
      "ctime": {
        "order": "desc"
      }
    }]
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    if len(res['hits']['hits']) > 0:
        res = res['hits']['hits'][0]['_source']
        msg = ["It's a match! Passenger {} and Passenger {} are going to {}".format(\
            res['id'], res['match'], res['destinationid']), res['location']]
    else:
        msg = ["Nothing happens", [0,0]]
    return msg

def arrivedMsg():
    q = { 'size': 1,
    'query' : {
            'term' : { 'status' : 'arrived' }
    },
     "filter": 
         {'query': {'exists' : { 'field' : 'match' }}
    },
     "sort": [{"ctime": {"order": "desc"}}]
         
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res
   