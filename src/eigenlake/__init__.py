from __future__ import annotations

from .client import EigenLakeClient
from . import schema


def connect(
    *,
    url: str,
    api_key: str | None = None,
    timeout: float = 20.0,
    retries: int = 2,
) -> EigenLakeClient:
    return EigenLakeClient(
        url=url,
        api_key=api_key,
        timeout=timeout,
        retries=retries,
    )


def connect_local(
    *,
    host: str = "http://localhost",
    port: int = 8000,
    api_key: str | None = None,
    timeout: float = 20.0,
    retries: int = 2,
) -> EigenLakeClient:
    return EigenLakeClient(
        url=f"{host.rstrip('/')}:{int(port)}",
        api_key=api_key,
        timeout=timeout,
        retries=retries,
    )


__all__ = [
    "EigenLakeClient",
    "connect",
    "connect_local",
    "schema",
]
