from __future__ import annotations

import time
from typing import Any

import httpx

from .errors import APIError, AuthenticationError, ConflictError, NetworkError, NotFoundError, ValidationError


class Transport:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None,
        timeout: float = 20.0,
        retries: int = 2,
    ):
        normalized = base_url.rstrip("/")
        self._retries = max(0, int(retries))
        self._client = httpx.Client(
            base_url=normalized,
            timeout=float(timeout),
            headers=self._auth_headers(api_key),
        )

    @staticmethod
    def _auth_headers(api_key: str | None) -> dict[str, str]:
        token = (api_key or "").strip()
        if not token:
            return {}
        return {"X-API-Key": token}

    @staticmethod
    def _detail(resp: httpx.Response) -> str:
        try:
            payload = resp.json()
            if isinstance(payload, dict) and payload.get("detail") is not None:
                return str(payload["detail"])
        except Exception:
            pass
        text = (resp.text or "").strip()
        return text or f"HTTP {resp.status_code}"

    def _raise_for_status(self, resp: httpx.Response) -> None:
        if 200 <= resp.status_code < 300:
            return

        detail = self._detail(resp)
        code = int(resp.status_code)

        if code in (401, 403):
            raise AuthenticationError(detail)
        if code == 404:
            raise NotFoundError(detail)
        if code == 409:
            raise ConflictError(detail)
        if code in (400, 422):
            raise ValidationError(detail)
        raise APIError(detail)

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        path = path if path.startswith("/") else f"/{path}"

        last_exc: Exception | None = None
        for attempt in range(self._retries + 1):
            try:
                resp = self._client.request(method.upper(), path, **kwargs)
            except httpx.RequestError as exc:
                last_exc = exc
                if attempt >= self._retries:
                    raise NetworkError(str(exc)) from exc
                time.sleep(0.2 * (attempt + 1))
                continue

            if resp.status_code >= 500 and attempt < self._retries:
                time.sleep(0.2 * (attempt + 1))
                continue

            self._raise_for_status(resp)
            return resp

        if last_exc is not None:
            raise NetworkError(str(last_exc)) from last_exc
        raise NetworkError("Request failed")

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", path, **kwargs)

    def close(self) -> None:
        self._client.close()
