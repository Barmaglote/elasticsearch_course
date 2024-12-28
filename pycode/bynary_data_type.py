from pprint import pprint
from elasticsearch import Elasticsearch
import base64

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='binary_index', ignore_unavailable=True)
es.indices.create(
    index='binary_index',
    mappings={
        "properties": {
            "image_data": {
                "type": "binary"
            }
        }
    })

image_path = "./images/field_data_types_docs.png"
with open(image_path, "rb") as f:
    image_data = f.read()
    response = es.index(
        index='binary_index',
        body={
            "image_data": base64.b64encode(image_data).decode('utf-8')
        }
    )
    print(response)
