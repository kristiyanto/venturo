from app import app
from flask import render_template, json
from elasticsearch import Elasticsearch

cluster = ['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106']
es = Elasticsearch(cluster, port=9200)


@app.route('/')
@app.route('/index')
def index():
    # return "Hello, World!"
    return render_template("index.html")
@app.route('/map')
def map():
    return render_template("index.html")

@app.route('/stats')
def getstats():
    drivers = activeDrivers()
    passenger = activePass()
    
    dLatLong = []

    if len(drivers['hits']['hits']) < 1:
        ad = 0

    else:
        ad = drivers['hits']['total']
        for i in res['hits']['hits']:
            dLatLong.append(i['_source']['location'])



    return json.dumps({"activeDrivers": ad, "activePassengers": dLatLong})

def activeDrivers():
    q = {
    "query" : {
        "constant_score" : {
            "filter" : {
                "range" : {
                    "ctime" : {
                        "gt"  : "now-2d"
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
    "query" : {
        "constant_score" : {
            "filter" : {
                "range" : {
                    "ctime" : {
                        "gt"  : "now-2d"
                    }
                }
            }
        }
    }
    }

    res = es.search(index='passenger', doc_type='rolling', body=q, ignore=[404, 400])
    return res

