from elasticsearch import Elasticsearch
import json

es = Elasticsearch('http://localhost:9200')
client_info = es.info()

dummy_data = json.load(open('./data/dummy_data.json'))

def insert_document(document):
    response = es.index(
        index='my_index',
        body=document
    )
    return response

def print_info(response):
    print(response["result"])

for docxum in dummy_data:
    response = insert_document(docxum)
    print_info(response)
