import json
from pprint import pprint
from elasticsearch import Elasticsearch


es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(
    index='my_index'
)

documents_ids = []
dummy_data = json.load(open('./data/dummy_data.json'))
for doc in dummy_data:
    response = es.index(
        index='my_index',
        body=doc
    )
    documents_ids.append(response['_id'])

print(documents_ids)

response = es.delete(
    index='my_index',
    id=documents_ids[0]
)

print(response)
pprint(response.body)


