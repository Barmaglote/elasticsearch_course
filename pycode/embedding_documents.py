from pprint import pprint
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import torch
import json
from tqdm import tqdm

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

index='my_index'

es.indices.delete(index=index, ignore_unavailable=True)
es.indices.create(
    index=index,
    mappings={
        "properties": {
            "embedding": {
                "type": "dense_vector"
            }
        }
    }
)

model = SentenceTransformer('all-MiniLM-L6-v2') # Use the all-MiniLM-L6-v2 model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

documents = json.load(open('./data/dummy_data.json'))

def get_embedding(text):
    print(text)
    result = model.encode(text)
    print(result)
    return result

operations = []
for doc in tqdm(documents, total=len(documents)):
    operations.append({
        "index": {
            "_index": index
        }
    })
    operations.append({
        **doc,
        "embedding": get_embedding(doc['text'])
    })

print(operations)
response=es.bulk(operations=operations)
print(response.body)
es.indices.refresh(index=index)

response=es.search(
    index=index,
    body={
        "query": {
            "match_all": {}
        }
    }
)

print(response['hits']['total']['value'])

response=es.indices.get_mapping(
    index=index
)

pprint(response.body)