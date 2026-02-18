from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ApiKeyAuth:
    key: str


class Auth:
    @staticmethod
    def api_key(value: str) -> ApiKeyAuth:
        token = (value or "").strip()
        if not token:
            raise ValueError("API key cannot be empty")
        return ApiKeyAuth(key=token)
