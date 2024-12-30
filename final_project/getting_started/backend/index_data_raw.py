import json
from pprint import pprint
from typing import List

from elasticsearch import Elasticsearch
from tqdm import tqdm

from utils import get_es_client
from config import INDEX_NAME_RAW

def index_data(documents: List[dict]):
    pipeline_id='apod_pipeline'
    es = get_es_client(max_retries=5, sleep_time=5)
    _create_pipeline(es=es, pipeline_id=pipeline_id)
    _create_index(es=es)
    _insert_documents(es=es, documents=documents, pipeline_id=pipeline_id)
    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{INDEX_NAME_RAW}"')

def _create_pipeline(es: Elasticsearch, pipeline_id: str):
    pipeline_body = {
        "description": "Pipeline that strips HTML tags from the explanation and title fields",
        "processors": [
            {
                "html_strip": {
                    "field": "explanation"
                }
            },
            {
                "html_strip": {
                    "field": "title"
                }
            }
        ]
    }
    return es.ingest.put_pipeline(id=pipeline_id, body=pipeline_body)

def _create_index(es: Elasticsearch) -> dict:
    return es.indices.create(
        index=INDEX_NAME_RAW
    )

def _insert_documents(es: Elasticsearch, documents: List[dict], pipeline_id: str):
    operations = []
    for document in tqdm(documents, total=len(documents), desc='Indexing documents'):
        operations.append({'index': {'_index': INDEX_NAME_RAW}})
        operations.append(document)
    return es.bulk(operations=operations, pipeline=pipeline_id)

if __name__ == '__main__':
    with open('../../../data/apod.json') as f:
        documents = json.load(f)

    index_data(documents=documents, use_n_gram_tokenizer=True)

