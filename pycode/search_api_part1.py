import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='index_1', ignore_unavailable=True)
es.indices.create(index='index_1')

es.indices.delete(index='index_2', ignore_unavailable=True)
es.indices.create(index='index_2')

def insert_document(document, index):
    response = es.index(
        index=index,
        body=document
    )
    return response

dummy_data = json.load(open('./data/dummy_data.json'))

for doc in dummy_data:
    print(insert_document(doc, "index_1"))

for doc in dummy_data:
    print(insert_document(doc, "index_2"))

es.indices.refresh(index='index_1')
es.indices.refresh(index='index_2')

# Search 1
response1 = es.search(
    index='index_1',
    body={
        "query": {
            "match_all": {}
        }
    }
)
n_hits1 = response1['hits']['total']['value']
print(n_hits1)

# Search 2
response2 = es.search(
    index='index_2',
    body={
        "query": {
            "match_all": {}
        }
    }
)

n_hits2 = response2['hits']['total']['value']
print(n_hits2)

# Search 3
response3 = es.search(
    index='index_1,index_2',
    body={
        "query": {
            "match_all": {}
        }
    }
)

n_hits3 = response3['hits']['total']['value']
print(n_hits3)

# Search 4
response4 = es.search(
    index='index*',
    body={
        "query": {
            "match_all": {}
        }
    }
)

n_hits4 = response4['hits']['total']['value']
print(n_hits4)

# Search 5
response5 = es.search(
    index='_all',
    body={
        "query": {
            "match_all": {}
        }
    }
)

n_hits5 = response5['hits']['total']['value']
print(n_hits5)
