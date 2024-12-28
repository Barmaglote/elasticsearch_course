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


ids=[]
def insert_document(document):
    response = es.index(
        index=index,
        body=document
    )
    ids.append(response['_id'])
    return response

dummy_data = json.load(open('./data/dummy_data.json'))
for doc in dummy_data:
    print(insert_document(doc))

es.indices.refresh(index=index)

response = es.indices.exists(
    index=index
)

print(response)

response = es.exists(
    index=index,
    id=ids[0]
)

print(response)