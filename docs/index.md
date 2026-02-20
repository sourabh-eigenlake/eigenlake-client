# EigenLake Python Client

`eigenlake` is the Python SDK for interacting with an EigenLake API server.

## Install

```bash
pip install eigenlake
```

## Quick Connect

```python
import eigenlake
from eigenlake import schema as s

api_key = "<sk_sbx_your_api_key_here>"
client = eigenlake.connect(
    url="https://api.eigenlake.dev/",
    api_key=api_key,
)

print(client.ready())
client.close()
```

## What You Get

- Connect to cloud or local EigenLake deployments
- Manage indexes by namespace
- Insert and search vectors
- Batch write helper for high-throughput inserts
