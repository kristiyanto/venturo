from app import app
from flask import render_template, json
from elasticsearch import Elasticsearch

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', 'ip-172-31-0-105', 'ip-172-31-0-106']
es = Elasticsearch(cluster, port=9200)
size = 5000
window = 'now-2h'

@app.route('/')
@app.route('/index')
@app.route('/index.html')
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
    onWait = waitPass()
    onRide = ridingPass()
    match = matches()
    arr = arrived()
    idle = idleDrivers()
    avg_wait = avgWait()
    msg = matchMsg()[0]
    msgLoc = matchMsg()[1]
    arrMsg = arrivedMsg()[0]
    arrMsgLoc = arrivedMsg()[1]

    
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
                       'dLoc': dLatLong, 'actPass': ap, \
                       'pLoc': pLatLong, 'onWait': onWait, 'onRide': onRide})

def activeDrivers():
    q = {"size": size, "filter": {"range": { "ctime": { "gt": window }}}}

    res = es.search(index='driver', doc_type='rolling', body=q, ignore=[404, 400])
    return res


def activePass():
    q = {"size": size, "filter": {"range": { "ctime": { "gt": window }}}}

    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res

def idleDrivers():
    q = {
        'query': { 'term': {'status': 'idle'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            }
        }
    res = es.search(index='driver', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]


def waitPass():
    q = {
        'query': { 'term': {'status': 'wait'} },
        'filter': {'range': { 'ctime': { 'gt': window }} 
            }
        }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]

def ridingPass():
    q = {
    'query': { 'term': {'status': 'ontrip'} },
    'filter': {'range': { 'ctime': { 'gt': window }} 
        }
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res['hits']["total"]

def arrived():
    q = {
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
        res = round(float(res['aggregations']['avg_wait']['avg_wait']['value']), 2)
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
    "sort": [
    {
      "ctime": {
        "order": "desc"
      }}]
    }
    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])


    if len(res['hits']['hits']) > 0:
        res = res['hits']['hits'][0]['_source']
        msg = ["Driver {} just arrived to {}".format(\
            res['id'], res['destinationid']), res['destination']]
    else:
        msg = ["Nothing happens", [0,0]]
    return msg
   