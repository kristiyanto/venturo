
# coding: utf-8

# In[314]:

from elasticsearch import Elasticsearch
es = Elasticsearch(['ip-172-31-0-107', 'ip-172-31-0-100', ' ip-172-31-0-105', 'ip-172-31-0-106'], port=9200)

driver_mapping = {
  'settings' : {
        'number_of_shards' : 20,
        'number_of_replicas' : 2
  },
  'mappings': {
    'rolling': {
      'properties': {
        'city': {'type': 'string',  "include_in_all": "false"},
        'id': {'type': 'string'},
        'status': {'type': 'string'},
        'location': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'ctime': {'type': 'date'},
        'p1': {'type': 'string', "include_in_all": "false"},
        'p2': {'type': 'string', "include_in_all": "false"},
        'destination': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'destinationid':{'type': 'string', "include_in_all": "false"},
        'origin': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
      }
    }
  }
}


pass_mapping = {
  'settings' : {
        'number_of_shards' : 20,
        'number_of_replicas' : 2
    },  
  'mappings': {
    'rolling': {
      'properties': {
        'city': {'type': 'string',  "include_in_all": "false"},
        'id': {'type': 'string'},
        'status': {'type': 'string'},
        'match': {'type': 'string', "include_in_all": "false"},                
        'location': {'type': 'geo_point', 'lat_lon': 'true'},
        'ctime': {'type': 'date'},
        'ptime': {'type': 'integer', "include_in_all": "false"},
        'atime': {'type': 'date', "include_in_all": "false"},
        'driver': {'type': 'string', "include_in_all": "false"},
        'destination': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'destinationid':{'type': 'string', "include_in_all": "false"},                
        'altdest1': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'altdestid1':{'type': 'string', "include_in_all": "false"},                    
        'altdest2': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'altdestid2':{'type': 'string', "include_in_all": "false"}, 
        'origin': {'type': 'geo_point', 'lat_lon': 'true', "include_in_all": "false"},
        'path': {'type': 'string', "include_in_all": "false"}
      }
    }
  }
}




# In[315]:

es.indices.delete(index='driver', ignore=[400, 404])
es.indices.delete(index='passenger', ignore=[400, 404])


# In[316]:

es.indices.create(index='driver', body=driver_mapping, ignore=400)
es.indices.create(index='passenger', body=pass_mapping, ignore=400)


# In[ ]:




# In[ ]:



