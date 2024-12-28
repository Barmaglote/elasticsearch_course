from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='text_index', ignore_unavailable=True)
es.indices.create(
    index='text_index',
    mappings={
        "properties": {
            "email_body": {
                "type": "text"
            }
        }
    })

document = {
    "email_body": "Hello, this is a text email"
}

response = es.index(
    index='text_index',
    body=document
)

print(response)


