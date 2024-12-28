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

dummy_data = json.load(open('./data/dummy_data_2.json'))

operations = []
for _ in range(50):
    for doc in dummy_data:
        operations.append({
            "index": {
                "_index": index
            }
        })
        operations.append(doc)

es.bulk(operations=operations)
print(len(operations))

es.indices.refresh(index=index)


# Size and from (pagination)
response = es.search(
    index=index,
    body={
        "query": {
            "match_all": {}
        },
        "size":10,
        "from":10
    }
)

n_hits = response['hits']['total']['value']
print(n_hits)

for hit in response['hits']['hits']:
    print(hit['_source'])

# timeout
response = es.search(
    index=index,
    body={
        "query": {
            "match_all": {}
        },
        "timeout": "10s"
    }
)

print(response.body)

n_hits = response['hits']['total']['value']
print(n_hits)

for hit in response['hits']['hits']:
    print(hit['_source'])

# aggrigations

response = es.search(
    index=index,
    body={
        "query": {
            "match_all": {}
        },
        "aggs": {
            "avg_age": {
                "avg": {
                    "field": "age"
                }
            }
        }
    }
)

avg_age = response['aggregations']['avg_age']['value']
print(avg_age)


# combine size, from, timeout and aggregations
response = es.search(
    index=index,
    body={
        "query": {
            "match": {
                "message": "important keyword"
            }
        },
        "aggs": {
            "max_price": {
                "max": {
                    "field": "price"
                }
            }
        },
        "size":5,
        "from":20,
        "timeout": "5s"
    }
)

for hit in response['hits']['hits']:
    print(hit['_source'])

avg_age = response['aggregations']['max_price']['value']
print(avg_age)


