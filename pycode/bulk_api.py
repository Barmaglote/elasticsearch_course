import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

index='my_bulk_index'

es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(
    index=index,
    settings={
        'number_of_shards': 3,
        'number_of_replicas': 1
    },
    mappings={
        "properties": {
            "created_on": {
                "type": "date"
            }
        }
    }
)


responses = es.bulk(
    operations=[
        {   # Action 1
            "index": {
                "_index": index,
                "_id": 1
            }
        },
        {   # Source 1
            "created_on": "2017-01-01"
        },
        {   # Action 2
            "index": {
                "_index": index,
                "_id": 2
            }
        },
        {   # Source 2
            "created_on": "2017-01-02"
        },
        {   # Action 3
            "update": {
                "_index": index,
                "_id": 1
            }
        },
        {   # Source 3
            "doc": {
                "title": "New title"
            }
        },
        {   # Action 4
            "update": {
                "_index": index,
                "_id": 1
            }
        },
        {   # Source 4
            "doc": {
                "new_field": "dummy_value"
            }
        },
        {   # Action 5
            "update": {
                "_index": index,
                "_id": 3
            }
        }
    ]
)

print(responses)

response = es.get(
    index=index,
    id=1
)

print(response)

