from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='geo_point_index', ignore_unavailable=True)
es.indices.create(
    index='geo_point_index',
    mappings={
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    })

document = {
    "text": "Geopoint as an object using GeoJSON format",
    "location": {
        "type": "Point",
        "coordinates": [-71.34, 41.12]
    }
}

response = es.index(index='geo_point_index', body=document)
response




