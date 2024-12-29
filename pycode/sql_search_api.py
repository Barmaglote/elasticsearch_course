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
documents = json.load(open("./data/astronomy.json"))

for document in documents:
    operations.append({'index': {'_index': 'my_index'}})
    operations.append(document)

response = es.bulk(operations=operations)
pprint(response.body)

es.indices.refresh(index=index)

count = es.count(index=index)['count']
print(count)

query = {
    "query": "SELECT * FROM my_index ORDER BY id LIMIT 5"
}

response = es.sql.query(
    body=query,
    format='json',
    filter={
        "term": {
            "title.keyword": "Black Holes"
        }
    },
)
for row in response['rows']:
    print(row)

# Pagination

query = {
    "query": "SELECT * FROM my_index ORDER BY id DESC"
}

response = es.sql.query(
    body=query,
    format='json',
    fetch_size=5,
)
response.body

# Cursor
while 'cursor' in response.body:
    response = es.sql.query(
        format='json',
        cursor=response.body['cursor'],
    )
print(response.body)

# Translate
translate_query = {
    "query": "SELECT * FROM my_index WHERE content LIKE '%universe%' ORDER BY id DESC LIMIT 20"
}

translated_query = es.sql.translate(body=translate_query)
translated_query.body