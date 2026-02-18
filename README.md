# eigenlake

Python SDK for Eigenlake Cloud.

## Install

```bash
pip install eigenlake
```

## Usage

```python
import os
import eigenlake
from eigenlake.classes.init import Auth

# Best practice: store your credentials in environment variables
cluster_url = os.environ["EIGENLAKE_URL"]
api_key = os.environ["EIGENLAKE_API_KEY"]

client = eigenlake.connect_to_eigenlake_cloud(
    cluster_url=cluster_url,
    auth_credentials=Auth.api_key(api_key),
)

print(client.is_ready())  # True

client.close()
```

## Documentation (MkDocs)

```bash
pip install -e ".[docs]"
mkdocs serve
```

Docs will be available at `http://127.0.0.1:8000` by default.
