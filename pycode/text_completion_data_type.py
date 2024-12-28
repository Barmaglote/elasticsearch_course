from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='text_completion_index', ignore_unavailable=True)
es.indices.create(
    index='text_completion_index',
    mappings={
        "properties": {
            "suggest": {
                "type": "completion"
            }
        }
    })

document_1 = {
    "suggest": ["Mars", "Planet"]
}

document_2 = {
    "suggest": ["Andromeda", "Galaxy"]
}

es.index(index='text_completion_index', body=document_1)
es.index(index='text_completion_index', body=document_2)



