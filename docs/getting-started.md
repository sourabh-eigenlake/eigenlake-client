# Getting Started

## Connect to EigenLake Cloud

```python
import eigenlake
from eigenlake import schema as s

api_key = "<sk_sbx_your_api_key_here>"
client = eigenlake.connect(
    url="https://api.eigenlake.dev/",
    api_key=api_key,
)
```

## Create an Index

```python
schema, index_options = (
    s.SchemaBuilder(additional_properties=False)
    .add("document_id", s.string(required=True, filterable=True))
    .add("document_title", s.string(filterable=True))
    .add("chunk_number", s.integer(filterable=True))
    .add("document_url", s.string(format="uri", filterable=True))
    .add("created_at", s.datetime(filterable=True))
    .add("tags", s.array(s.string(), filterable=False, max_items=20))
    .build()
)

index = client.indexes.create_or_get(
    namespace="demo-namespace",
    index="demo-index",
    dimensions=128,
    schema=schema,
    index_options=index_options,
)
```

## Insert a Vector

```python
record_id = index.records.add(
    properties={"document_id": "doc-1", "text": "hello"},
    vector=[0.1] * 128,
)
print("inserted:", record_id)
```

## Query by Vector

```python
result = index.search.nearest(
    vector=[0.1] * 128,
    limit=5,
)
print(result)
```

## Close the Client

```python
client.close()
```
