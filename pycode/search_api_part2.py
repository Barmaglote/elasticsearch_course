import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)
index='index_1'

es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(index=index)

def insert_document(document, index):
    response = es.index(
        index=index,
        body=document
    )
    return response

dummy_data = json.load(open('./data/dummy_data.json'))

for doc in dummy_data:
    print(insert_document(doc, index))

es.indices.refresh(index=index)

response = es.search(
    index=index,
    body={
        "query": {
            "term": {
                "created_on": "2024-09-22"
            }
        }
    }
)

n_hits = response['hits']['total']['value']
print(n_hits)

response = es.search(
    index=index,
    body={
        "query": {
            "match": {
                "text": "document"
            }
        }
    }
)

n_hits = response['hits']['total']['value']
print(n_hits)

response = es.search(
    index=index,
    body={
        "query": {
            "range": {
                "created_on": {
                    "lte": "2024-09-23"
                }
            }
        }
    }
)

n_hits = response['hits']['total']['value']
print(n_hits)

response = es.search(
    index=index,
    body={
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "text": "third"
                        },
                    },
                    {
                        "range": {
                            "created_on": {
                                "gte": "2024-09-24",
                                "lte": "2024-09-24"
                            }
                        }
                    }
                ]
            }
        }
    }
)

n_hits = response['hits']['total']['value']
print(n_hits)