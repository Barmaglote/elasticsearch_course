from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='point_index', ignore_unavailable=True)
es.indices.create(
    index='point_index',
    mappings={
        "properties": {
            "location": {
                "type": "point"
            }
        }
    })

document = {
    "location": {
        "type": "Point",
        "coordinates": [-71.34, 41.12]
    }
}

response = es.index(index='point_index', body=document)
response




