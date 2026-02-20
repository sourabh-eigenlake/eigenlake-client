# eigenlake

Python SDK for EigenLake Cloud.

## Install

```bash
pip install eigenlake
```

## Usage

```python
import os
import eigenlake
from eigenlake import schema as s

# Best practice: store your credentials in environment variables
url = os.environ["EIGENLAKE_URL"]
api_key = os.environ["EIGENLAKE_API_KEY"]

client = eigenlake.connect(
    url=url,
    api_key=api_key,
)

print(client.ready())  # True

schema, index_options = (
    s.SchemaBuilder(additional_properties=False)
    .add("document_id", s.string(required=True, filterable=True))
    .add("document_title", s.string(filterable=True))
    .add("chunk_number", s.integer(filterable=True))
    .build()
)

index = client.indexes.create_or_get(
    namespace="demo-namespace",
    index="demo-index",
    dimensions=128,
    schema=schema,
    index_options=index_options,
)

client.close()
```

## Documentation (MkDocs)

```bash
pip install -e ".[docs]"
mkdocs serve
```

Docs will be available at `http://127.0.0.1:8000` by default.
