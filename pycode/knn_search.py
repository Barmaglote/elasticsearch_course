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

documents = json.load(open('./data/astronomy.json'))

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
        "embedding": get_embedding(doc['content'])
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

# Search 1
query = "What is a black hole?"
embedded_query = get_embedding(query)
response = es.search(
    index=index,
    knn={
        "field": "embedding",
        "query_vector": embedded_query,
        "num_candidates": 5,
        "k": 3
    }
)

print(response.body["_shards"]["successful"])
hits=response['hits']['hits']

for hit in hits:
    print(hit['_source']['title'])
    print(hit['_source']['content'])
    print(hit['_score'])
    print("*"*100)

# Search 2
query = "How do we find exoplanets?"
embedded_query = get_embedding(query)
response = es.search(
    index=index,
    knn={
        "field": "embedding",
        "query_vector": embedded_query,
        "num_candidates": 5,
        "k": 3
    }
)

print(response.body["_shards"]["successful"])
hits=response['hits']['hits']

for hit in hits:
    print(hit['_source']['title'])
    print(hit['_source']['content'])
    print(hit['_score'])
    print("*"*100)

# Search 3
query = "What is dark matter?"
embedded_query = get_embedding(query)
response = es.search(
    index=index,
    knn={
        "field": "embedding",
        "query_vector": embedded_query,
        "num_candidates": 5,
        "k": 1
    }
)

print(response.body["_shards"]["successful"])
hits=response['hits']['hits']

for hit in hits:
    print(hit['_source']['title'])
    print(hit['_source']['content'])
    print(hit['_score'])
    print("*"*100)
