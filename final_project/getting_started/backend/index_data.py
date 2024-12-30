import json
from pprint import pprint
from typing import List

from elasticsearch import Elasticsearch
from tqdm import tqdm

from utils import get_es_client
from config import INDEX_NAME_DEFAULT, INDEX_NAME_N_GRAM

def index_data(documents: List[dict], use_n_gram_tokenizer: bool = False):
    es = get_es_client(max_retries=5, sleep_time=5)
    _create_index(es=es, use_n_gram_tokenizer=use_n_gram_tokenizer)
    _insert_documents(es=es, documents=documents, use_n_gram_tokenizer=use_n_gram_tokenizer)
    index_name = INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME_DEFAULT
    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{index_name}"')

def _create_index(es: Elasticsearch, use_n_gram_tokenizer: bool = False) -> dict:
    tokenizer = 'n_gram_tokenizer' if use_n_gram_tokenizer else 'standard'
    index_name = INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME_DEFAULT
    _ = es.indices.delete(index=index_name, ignore_unavailable=True)

    return es.indices.create(index=index_name, body={
        'settings': {
            'analysis': {
                'analyzer': {
                    'default': {
                        'type': 'custom',
                        'tokenizer': tokenizer,
                    },
                },
                'tokenizer': {
                    'n_gram_tokenizer': {
                        'type': 'nGram',
                        'min_gram': 2,
                        'max_gram': 20,
                        'token_chars': ['letter', 'digit']
                    }
                }
            }
        },
    })

def _insert_documents(es: Elasticsearch, documents: List[dict], use_n_gram_tokenizer: bool = False):
    index_name = INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME_DEFAULT
    operations = []
    for document in tqdm(documents, total=len(documents), desc='Indexing documents'):
        operations.append({'index': {'_index': index_name}})
        operations.append(document)
    return es.bulk(operations=operations)

if __name__ == '__main__':
    with open('../../../data/apod.json') as f:
        documents = json.load(f)

    index_data(documents=documents, use_n_gram_tokenizer=True)