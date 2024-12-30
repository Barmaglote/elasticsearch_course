from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

# 1. Character filters
## 1.1. HTML Strip Character Filter

response = es.indices.analyze(
    char_filter=[
        "html_strip"
    ],
    text="I&apos;m so happy</b>!</p>",
)
print(response.body)

## 1.2. Mapping character filter

response = es.indices.analyze(
    tokenizer="keyword",
    char_filter=[
        {
            "type": "mapping",
            "mappings": [
                "٠ => 0",
                "١ => 1",
                "٢ => 2",
                "٣ => 3",
                "٤ => 4",
                "٥ => 5",
                "٦ => 6",
                "٧ => 7",
                "٨ => 8",
                "٩ => 9"
            ]
        }
    ],
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
print(response.body)

# 2. Tokenizer

## 2.1. Standard

response = es.indices.analyze(
    tokenizer="standard",
    text="The 2 QUICK Brown-Foxes jumped over the lazy dog's bone.",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}', Type: {token['type']}")


## 2.2. Lowercase

response = es.indices.analyze(
    tokenizer="lowercase",
    text="The 2 QUICK Brown-Foxes jumped over the lazy dog's bone.",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}', Type: {token['type']}")

## 2.3. Whitespace
response = es.indices.analyze(
    tokenizer="whitespace",
    text="The 2 QUICK Brown-Foxes jumped over the lazy dog's bone.",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}', Type: {token['type']}")

# 3. Token filter
## 3.1. Apostrophe

response = es.indices.analyze(
    tokenizer="standard",
    filter=[
        "apostrophe"
    ],
    text="The 2 QUICK Brown-Foxes jumped over the lazy dog's bone.",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

## 3.2. Decimal digit
response = es.indices.analyze(
    tokenizer="standard",
    filter=[
        "decimal_digit"
    ],
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

## 3.3. Reverse
response = es.indices.analyze(
    tokenizer="standard",
    filter=[
        "reverse"
    ],
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

# 4. Built-in analyzers
## 4.1. Standard
response = es.indices.analyze(
    analyzer="standard",
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

## 4.2. Stop
response = es.indices.analyze(
    analyzer="stop",
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

## 4.3. Keyword
response = es.indices.analyze(
    analyzer="keyword",
    text="I saw comet Tsuchinshan Atlas in ٢٠٢٤",
)
tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")


# 5. Index time VS Search time analysis

## 5.1. Index time
index_name = "index_time_example"
settings = {
    "settings": {
        "analysis": {
            "char_filter": {
                "ampersand_replacement": {
                    "type": "mapping",
                    "mappings": ["& => and"]
                }
            },
            "analyzer": {
                "custom_index_analyzer": {
                    "type": "custom",
                    "char_filter": ["html_strip", "ampersand_replacement"],
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "custom_index_analyzer"
            }
        }
    }
}

es.indices.delete(index=index_name, ignore_unavailable=True)
es.indices.create(index=index_name, body=settings)

document = {
    "content": "Visit my website https://myuniversehub.com/ & like some images!"}
response = es.index(index=index_name, id=1, body=document)
print(response.body)

es.indices.refresh(index=index_name)

response = es.search(index=index_name, body={"query": {"match_all": {}}})
hits = response.body["hits"]["hits"]

for hit in hits:
    print(hit["_source"])

response = es.indices.analyze(
    index=index_name,
    body={
        "field": "content",
        "text": "Visit my website https://myuniversehub.com/ & like some images!"
    }
)

tokens = response.body["tokens"]
for token in tokens:
    print(f"Token: '{token['token']}'")

## 5.2. Search time
response = es.search(index=index_name, body={
    "query": {
        "match": { # match is used for full-text search
            "content": {
                "query": "myuniversehub.com",
                "analyzer": "standard"  # Using a different analyzer than the one used at index time
            }
        }
    }
})

hits = response["hits"]["hits"]
for hit in hits:
    print(hit["_source"])

response = es.search(index=index_name, body={
    "query": {
        "term": {  # term is used for exact matches
            "content": {
                "value": "MYUNIVERSEHUB.com",
            }
        }
    }
})

hits = response["hits"]["hits"]
for hit in hits:
    print(hit["_source"])