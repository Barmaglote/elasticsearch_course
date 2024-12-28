from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='other_common_data_types_index', ignore_unavailable=True)
es.indices.create(
    index='other_common_data_types_index',
    mappings={
        "properties": {
            "book_reference": {
                "type": "keyword"
            },
            "price": {
                "type": "float"
            },
            "publish_date": {
                "type": "date"
            },
            "is_available": {
                "type": "boolean"
            }
        }
    })

response = es.index(
    index='other_common_data_types_index',
    body={
        "book_reference": "123-456789",
        "price": 9.99,
        "publish_date": "2022-01-01",
        "is_available": True
    }
)

print(response)