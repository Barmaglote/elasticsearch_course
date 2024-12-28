import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)
index='index_1'

es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(
    index=index,
    mappings={
        "properties": {
            "sides_length": {
                "type": "dense_vector",
                "dims": 4
            },
            "shape": {
                "type": "keyword",
            }
        }
    }
)

response = es.index(
    index=index,
    body={
        "sides_length": [5, 5, 5, 5],
        "shape": "square"
    }
)

print(response.body)

pprint(es.indices.get_mapping(index=index).body)

# invalid dense vector
response = es.index(
    index=index,
    body={
        "sides_length": [[5, 5], [5, 5]],
        "shape": "square"
    }
)