from __future__ import annotations

from .classes.init import Auth
from .client import Client


def connect_to_eigenlake_cloud(
    *,
    cluster_url: str,
    auth_credentials=None,
    timeout: float = 20.0,
    retries: int = 2,
) -> Client:
    return Client(
        cluster_url=cluster_url,
        auth_credentials=auth_credentials,
        timeout=timeout,
        retries=retries,
    )


def connect_to_local(
    *,
    host: str = "http://localhost",
    port: int = 8000,
    auth_credentials=None,
    timeout: float = 20.0,
    retries: int = 2,
) -> Client:
    return Client(
        cluster_url=f"{host.rstrip('/')}:{int(port)}",
        auth_credentials=auth_credentials,
        timeout=timeout,
        retries=retries,
    )


__all__ = [
    "Auth",
    "Client",
    "connect_to_eigenlake_cloud",
    "connect_to_local",
]
