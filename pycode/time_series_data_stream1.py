from pprint import pprint
from elasticsearch import Elasticsearch
import psutil
from datetime import datetime, timezone

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch version', client_info['version']['number'])

# create a policy

policy = {
    "phases": {
        "hot": {
            "actions": {
                "rollover": {
                    "max_age": "5m",
                }
            }
        },
        "delete": {
            "min_age": "20m",
            "actions": {
                "delete": {}
            }
        }
    }
}

response = es.ilm.put_lifecycle(name="cpu_usage_policy_v2", policy=policy)
pprint(response.body)

# create an index template

response = es.indices.put_index_template(
    name="cpu_example_template",
    index_patterns=[
        "cpu_example_template*"  # Applies to any index starting with 'cpu_example_template'
    ],
    data_stream={},
    template={
        "settings": {
            "index.mode": "time_series",  # Enable time series data mode
            "index.lifecycle.name": "cpu_usage_policy_v2",  # Apply the ILM policy
        },
        "mappings": {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "cpu_usage": {
                    "type": "float",
                    "time_series_metric": "gauge"
                },
                "cpu_name": {
                    "type": "keyword",
                    "time_series_dimension": True
                }
            }
        }
    },
    priority=500,  # A priority higher than 200 to avoid collisions with built-in templates
    meta={
        "description": "Template for CPU usage data",
    },
    allow_auto_create=True
)
pprint(response.body)


index_alias = "cpu_example_template"
while True:
    document = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "cpu_usage": psutil.cpu_percent(interval=0.1),
        "cpu_name": "i7-13650HX"
    }

    es.index(index=index_alias, document=document, refresh=True)

