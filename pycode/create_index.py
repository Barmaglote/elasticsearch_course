from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(
    index='my_index',
    settings={
        'number_of_shards': 3,
        'number_of_replicas': 2
    })
es.indices.refresh(index='my_index')


