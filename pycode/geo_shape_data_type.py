from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])
pprint(client_info)

es.indices.delete(index='geo_shape_index', ignore_unavailable=True)
es.indices.create(
    index='geo_shape_index',
    mappings={
        "properties": {
            "location": {
                "type": "geo_shape"
            }
        }
    })

document_1 = {
    "location": {
        "type": "LineString",
        "coordinates": [[-77.03653, 38.897676], [-77.009051, 38.889939]]
    }
}

document_2 = {
    "location": {
        "type": "Polygon",
        "coordinates": [
            [
                [100, 0],
                [101, 0],
                [101, 1],
                [100, 1],
                [100, 0],
            ],
            [
                [100.2, 0.2],
                [100.8, 0.2],
                [100.8, 0.8],
                [100.2, 0.8],
                [100.2, 0.2],
            ]
        ]
    }
}

es.index(index='geo_shape_index', body=document_1)
es.index(index='geo_shape_index', body=document_2)





