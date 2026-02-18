# EigenLake Python Client

`eigenlake` is the Python SDK for interacting with an EigenLake API server.

## Install

```bash
pip install eigenlake
```

## Quick Connect

```python
import eigenlake
from eigenlake.classes.init import Auth

api_key = "<sk_sbx_your_api_key_here>"
client = eigenlake.connect_to_eigenlake_cloud(
    cluster_url="https://api.eigenlake.dev/",
    auth_credentials=Auth.api_key(api_key),
)

print(client.is_ready())
client.close()
```

## What You Get

- Connect to cloud or local EigenLake deployments
- Manage collections
- Insert and query vectors
- Batch write helper for high-throughput inserts

