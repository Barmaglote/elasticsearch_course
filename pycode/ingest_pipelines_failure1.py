import json
from pprint import pprint

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

pipeline='my_pipeline'

response = es.ingest.put_pipeline(
    id=pipeline,
    description='This pipeline transforms all text to lowercase',
    processors=[
        {
            "lowercase": {
                "field": "text"
            },
        },
        {
            "set": {
                "field": "text",
                "value": "CHANGED BY PIPELINE"
            }

        }
    ]
)

pprint(response.body)

response = es.ingest.get_pipeline(id='lowercase_pipeline')
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

pprint(response.response)