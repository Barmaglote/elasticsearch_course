import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

index='my_count_index'

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

def insert_document(document):
    response = es.index(
        index=index,
        body=document
    )
    return response

dummy_data = json.load(open('./data/dummy_data.json'))
for doc in dummy_data:
    print(insert_document(doc))

query = {
    "range": {
        "created_on": {
            "gte": "2024-09-22",
            "lte": "2024-09-23",
            "format": "yyyy-MM-dd"
        }
    }
}

es.indices.refresh(index=index)

response = es.count(
    index=index,
    query=query
)

print(response)
print(response["count"])

