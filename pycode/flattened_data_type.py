from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='flattened_object_index', ignore_unavailable=True)
es.indices.create(
    index='flattened_object_index',
    mappings={
        "properties": {
            "author": {
                "type": "flattened"
            }
        }
    })

document = {
    "author": {
        "first_name": "Imad",
        "last_name": "Saddik"
    }
}

response = es.index(
    index='flattened_object_index',
    body=document
)

print(response)


