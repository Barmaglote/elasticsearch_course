from pprint import pprint

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

pipeline_id = "multi_steps_pipeline"

document = {
    "price": "100.50",
    "old_name": "old_value",
    "description": "<p>This is a description with HTML.</p>",
    "username": "UserNAME",
    "category": "books",
    "title": "   Example Title with Whitespace   ",
    "tags": "tag1,tag2,tag3",
    "temporary_field": "This field should be removed"
}

pipeline_body = {
    "description": "Pipeline to demonstrate various ingest processors",
    "processors": [
        {
            "convert": {
                "field": "price",
                "type": "float",
                "ignore_missing": True
            }
        },
        {
            "rename": {
                "field": "old_name",
                "target_field": "new_name"
            }
        },
        {
            "set": {
                "field": "status",
                "value": "active"
            }
        },
        {
            "html_strip": {
                "field": "description"
            }
        },
        {
            "lowercase": {
                "field": "username"
            }
        },
        {
            "uppercase": {
                "field": "category"
            }
        },
        {
            "trim": {
                "field": "title"
            }
        },
        {
            "split": {
                "field": "tags",
                "separator": ","
            }
        },
        {
            "remove": {
                "field": "temporary_field"
            }
        },
        {
            "append": {
                "field": "tags",
                "value": ["new_tag"]
            }
        }
    ]
}

es.ingest.put_pipeline(id=pipeline_id, body=pipeline_body)
print(f"Pipeline '{pipeline_id}' created successfully!")

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index')

response = es.index(index="my_index", document=document, pipeline=pipeline_id)
pprint(response.body)

es.indices.refresh(index='my_index')

response = es.search(index='my_index')
hits = response.body['hits']['hits']

for hit in hits:
    pprint(hit['_source'])