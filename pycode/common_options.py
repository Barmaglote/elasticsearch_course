import json
from pprint import pprint

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

# 1. Human-readable output
response = es.cluster.stats(human=True)
pprint(response["nodes"]["jvm"])

response = es.cluster.stats(human=False)
pprint(response["nodes"]["jvm"])

# 2. Date math
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index')

operations = []
index_name = 'my_index'
dummy_data = json.load(open("./data/dummy_data.json"))
for document in dummy_data:
    operations.append({'index': {'_index': index_name}})
    operations.append(document)

es.bulk(operations=operations)

es.indices.refresh(index=index_name)

response = es.search(
    index=index_name,
    body={
        "query": {
            "range": {
                "created_on": {
                    "gte": "2024-09-22||+1d/d",  # 2024-09-23
                    "lte": "now/d"  # 2024-11-16
                }
            }
        }
    }
)
hits = response['hits']['hits']
print(f"Found {len(hits)} documents")

# 3. Response filtering
## 3.1 Inclusive filtering
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
)
pprint(response.body)

response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="hits.hits._id,hits.hits._source"  # Keep only _id and _source fields
)
pprint(response.body)

## 3.2 Exclusive filtering
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="-hits"  # Remove the hits key
)
pprint(response.body)

## 3.3. Combined filtering
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="hits.hits._id,-hits.hits._score"
)
pprint(response.body)

# 4. Flat settings
response = es.indices.get_settings(
    index=index_name,
)
pprint(response.body)

response = es.indices.get_settings(
    index=index_name,
    flat_settings=True,
)
pprint(response.body)