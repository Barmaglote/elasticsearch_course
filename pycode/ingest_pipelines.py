import json
from pprint import pprint

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

response = es.ingest.put_pipeline(
    id='lowercase_pipeline',
    description='This pipeline transforms all text to lowercase',
    processors=[
        {
            "lowercase": {
                "field": "text"
            }
        }
    ]
)

pprint(response.body)

response = es.ingest.get_pipeline(id='lowercase_pipeline')
pprint(response.body)


# delete pipeline
# response = es.ingest.delete_pipeline(id='lowercase_pipeline')
# pprint(response.body)

response = es.ingest.simulate(
    id='lowercase_pipeline',
    docs=[
        {
            "_index": "my_index",
            "_id": "1",
            "_source": {
                "text": "HELLO WORLD"
            }
        }
    ]
)

pprint(response.body)


dummy_data = json.load(open('./data/dummy_data.json'))
for i, document in enumerate(dummy_data):
    uppercased_text = document['text'].upper()
    document['text'] = uppercased_text
    dummy_data[i] = document

print(dummy_data)

index='my_index'
es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(index=index)

operations = []

for doc in dummy_data:
    operations.append({
        "index": {
            "_index": index
        }
    })
    operations.append(doc)

response = es.bulk(operations=operations, pipeline='lowercase_pipeline')
pprint(response.body)

es.indices.refresh(index=index)

response = es.search(index=index)
hits = response['hits']['hits']
for hit in hits:
    print(hit['_source'])

