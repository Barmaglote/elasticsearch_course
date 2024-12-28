from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()

index_mapping = es.indices.get(index='my_index')
pprint(index_mapping["my_index"])

