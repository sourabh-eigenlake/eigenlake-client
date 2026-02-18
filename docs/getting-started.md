# Getting Started

## Connect to EigenLake Cloud

```python
import eigenlake
from eigenlake.classes.init import Auth

api_key = "<sk_sbx_your_api_key_here>"
client = eigenlake.connect_to_eigenlake_cloud(
    cluster_url="https://api.eigenlake.dev/",
    auth_credentials=Auth.api_key(api_key),
)
```

## Create a Collection

```python
schema = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "document_id": {"type": "string"},
        "text": {"type": "string"},
    },
}

collection = client.collections.get_or_create(
    bucket_name="demo-bucket",
    name="demo-index",
    dims=128,
    schema_json=schema,
    sharded=True,
)
```

## Insert a Vector

```python
uid = collection.data.insert(
    properties={"document_id": "doc-1", "text": "hello"},
    vector=[0.1] * 128,
)
print("inserted:", uid)
```

## Query by Vector

```python
result = collection.query.near_vector(
    vector=[0.1] * 128,
    top_k=5,
)
print(result)
```

## Close the Client

```python
client.close()
```

