from pprint import pprint
from elasticsearch import Elasticsearch
import json


es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

index='my_index'

es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(index=index)

operations = []
clothes_documents = json.load(open("./data/clothes.json"))

for document in clothes_documents:
    operations.append({'index': {'_index': 'my_index'}})
    operations.append(document)

response = es.bulk(operations=operations)
pprint(response.body)

es.indices.refresh(index=index)

count = es.count(index=index)['count']
print(count)

response = es.search(
    index=index,
    query={
        "bool": {
            "filter": [
                {
                    "term": {
                        "color" : "yellow"
                    }
                },
                {
                    "term": {
                        "brand": "adidas"
                    }
                }
            ]
        }
    }
)
hits = response['hits']['hits']
for hit in hits:
    print(hit['_source'])

response = es.search(
    index="my_index",
    body={
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "brand": "gucci"
                    }
                }
            }
        },
        "aggs": {
            "colors": {
                "terms": {
                    "field": "color.keyword"
                }
            },
            "color_red": {
                "filter": {
                    "term": {
                        "color.keyword": "red"
                    }
                },
                "aggs": {
                    "models": {
                        "terms": {
                            "field": "model.keyword"
                        }
                    }
                }
            }
        },
        "post_filter": {
            "term": {
                "color": "red"
            }
        },
        "size": 20
    }
)
hits = response['hits']['hits']
for hit in hits:
    print(hit['_source'])

colors_aggregation = response.body['aggregations']['colors']['buckets']
pprint(colors_aggregation)

color_red_aggregation = response.body['aggregations']['color_red']['models']['buckets']
pprint(color_red_aggregation)

hits = response.body['hits']['hits']
for hit in hits:
    print(f"""Shirt brand: {hit['_source']['brand']}, color: {
          hit['_source']['color']}, and model: {hit['_source']['model']}""")