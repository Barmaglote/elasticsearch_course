from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='nested_object_index', ignore_unavailable=True)
es.indices.create(
    index='nested_object_index',
    mappings={
        "properties": {
            "user": {
                "type": "nested"
            }
        }
    })

documents = [
    {
        "first_name": "John",
        "last_name": "Smith"
    },
    {
        "first_name": "Imad",
        "last_name": "Saddik"
    },
]

response = es.index(
    index='nested_object_index',
    body={"user": documents}
)

print(response)


