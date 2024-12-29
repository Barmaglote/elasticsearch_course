import json
from pprint import pprint

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

pipeline='my_pipeline'

response = es.ingest.put_pipeline(
    id=pipeline,
    description='Pipeline with multiple transformations, handling failures',
    processors=[
        {
            "lowercase": {
                "field": "text",
                "on_failure": [
                    {
                        "set": {
                            "field": "text",
                            "value": "FAILED TO LOWERCASE",
                            "ignore_failure": True
                        }
                    }
                ]
            }
        },
        {
            "set": {
                "field": "new_field",
                "value": "ADDED BY PIPELINE",
                "ignore_failure": True
            }
        }
    ]
)

pprint(response.body)

response = es.ingest.get_pipeline(id=pipeline)
pprint(response.body)

document={
    "title": "Samnple Title 4",
    "created_on": "2024-09-25"
}

response = es.index(
    index='my_index',
    pipeline=pipeline,
    body=document
)

pprint(response.body)

response = es.search(index='my_index')
hits = response['hits']['hits']
for hit in hits:
    print(hit['_source'])