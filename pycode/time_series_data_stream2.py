from pprint import pprint
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch!')

count = es.count(index="cpu_example_template")
pprint(count.body)

response = es.search(
    index="cpu_example_template",
    body={
        "query": {
            "match_all": {}
        },
        "size": 1000
    },
)
hits = response.body["hits"]["hits"]

cpu_usage_values = [hit["_source"]["cpu_usage"] for hit in hits]
timestamp_values = [hit["_source"]["@timestamp"] for hit in hits]

plt.figure(figsize=(10, 5))
plt.plot(timestamp_values, cpu_usage_values)
plt.xticks([])
plt.xlabel("Timestamp")
plt.ylabel("CPU Usage (%)")
plt.title("CPU usage over time")
plt.grid(True)
plt.show()


response = es.search(
    index="cpu_example_template",
    body={
        "aggs": {
            "avg_cpu_usage": {
                "avg": {
                    "field": "cpu_usage"
                }
            }
        },
    },
)
average_cpu_usage = response.body["aggregations"]["avg_cpu_usage"]["value"]
print(f"Average CPU usage: {average_cpu_usage}%")


response = es.search(
    index="cpu_example_template",
    body={
        "aggs": {
            "max_cpu_usage": {
                "max": {
                    "field": "cpu_usage"
                }
            }
        },
    },
)
max_cpu_usage = response.body["aggregations"]["max_cpu_usage"]["value"]
print(f"Max CPU usage: {max_cpu_usage}%")


response = es.indices.get_data_stream()
pprint(response.body)

ilm_status = es.ilm.get_lifecycle(name="cpu_usage_policy_v2")
pprint(ilm_status.body)


response = es.ilm.explain_lifecycle(
    index=".ds-cpu_example_template*")
pprint(response.body)