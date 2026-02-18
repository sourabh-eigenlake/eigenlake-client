from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional
from urllib.parse import quote
from uuid import uuid4

from .classes.init import ApiKeyAuth
from .transport import Transport


def _q(value: str) -> str:
    return quote(str(value), safe="")


def _collection_path(bucket_name: str, name: str) -> str:
    return f"/v1/collections/{_q(bucket_name)}/{_q(name)}"


@dataclass
class FailedObject:
    uuid: str
    error: str


class InsertManyResult(list[str]):
    def __init__(self, uuids: Iterable[str], *, failed_objects: List[FailedObject] | None = None):
        super().__init__(uuids)
        self.failed_objects: List[FailedObject] = failed_objects or []

    @property
    def number_errors(self) -> int:
        return len(self.failed_objects)


class CollectionData:
    def __init__(self, handle: "CollectionHandle"):
        self._h = handle

    def insert(
        self,
        *,
        properties: Dict[str, Any],
        vector: List[float],
        uuid: Optional[str] = None,
        on_duplicate: Literal["error", "replace", "skip"] = "error",
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> str:
        payload = {
            "properties": properties,
            "vector": vector,
            "uuid": uuid,
            "on_duplicate": on_duplicate,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        resp = self._h._t.post(f"{self._h._path}/data/insert", json=payload).json()
        return str(resp["uuid"])

    def insert_many(
        self,
        objects: Iterable[dict[str, Any]],
        *,
        on_duplicate: Literal["error", "replace", "skip"] = "error",
        on_error: Literal["raise", "continue"] = "raise",
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> InsertManyResult:
        payload = {
            "objects": list(objects),
            "on_duplicate": on_duplicate,
            "on_error": on_error,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        resp = self._h._t.post(f"{self._h._path}/data/insert-many", json=payload).json()
        failed = [FailedObject(**x) for x in (resp.get("failed_objects") or [])]
        return InsertManyResult(resp.get("uuids") or [], failed_objects=failed)

    def insert_vectors(
        self,
        vectors: Iterable[dict[str, Any]],
        *,
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> None:
        payload = {
            "vectors": list(vectors),
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        self._h._t.post(f"{self._h._path}/data/insert-vectors", json=payload)

    def get_by_id(self, uuid: str, *, return_data: bool = True, return_metadata: bool = True) -> dict[str, Any] | None:
        payload = {
            "uuid": uuid,
            "return_data": return_data,
            "return_metadata": return_metadata,
        }
        resp = self._h._t.post(f"{self._h._path}/data/get-by-id", json=payload).json()
        return resp.get("object")

    def exists(self, uuid: str) -> bool:
        resp = self._h._t.get(f"{self._h._path}/data/exists/{_q(uuid)}").json()
        return bool(resp.get("exists", False))

    def delete_by_id(self, uuid: str, *, batch_size: int = 500) -> None:
        self._h._t.delete(f"{self._h._path}/data/{_q(uuid)}", params={"batch_size": batch_size})

    def delete_many(
        self,
        *,
        where: Dict[str, Any],
        limit: Optional[int] = None,
        delete_patent_rows: bool = False,
        on_missing_keys: Literal["skip", "error"] = "skip",
        batch_size: int = 500,
        background: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "where": where,
            "limit": limit,
            "delete_patent_rows": delete_patent_rows,
            "on_missing_keys": on_missing_keys,
            "batch_size": batch_size,
            "background": background,
        }
        return self._h._t.post(f"{self._h._path}/data/delete-many", json=payload).json()

    def get_delete_job(self, job_id: int) -> Dict[str, Any]:
        return self._h._t.get(f"{self._h._path}/data/delete-jobs/{int(job_id)}").json()

    def update(
        self,
        *,
        uuid: str,
        properties: Dict[str, Any] | None = None,
        vector: Optional[List[float]] = None,
    ) -> None:
        payload = {
            "properties": properties,
            "vector": vector,
        }
        self._h._t.patch(f"{self._h._path}/data/{_q(uuid)}", json=payload)

    def replace(
        self,
        *,
        uuid: str,
        properties: Dict[str, Any],
        vector: Optional[List[float]] = None,
    ) -> None:
        payload = {
            "properties": properties,
            "vector": vector,
        }
        self._h._t.put(f"{self._h._path}/data/{_q(uuid)}", json=payload)

    def get_by_filter(
        self,
        *,
        where: Dict[str, Any],
        limit: int = 100,
        after: Optional[str] = None,
        include_vector: bool = False,
        include_properties: bool = True,
        on_missing_keys: Literal["skip", "error"] = "skip",
    ) -> Dict[str, Any]:
        payload = {
            "where": where,
            "limit": limit,
            "after": after,
            "include_vector": include_vector,
            "include_properties": include_properties,
            "on_missing_keys": on_missing_keys,
        }
        return self._h._t.post(f"{self._h._path}/data/get-by-filter", json=payload).json()


class CollectionQuery:
    def __init__(self, handle: "CollectionHandle"):
        self._h = handle

    def near_vector(self, *, vector: List[float], top_k: int = 10, filter: Optional[Dict[str, Any]] = None):
        payload = {
            "vector": vector,
            "top_k": top_k,
            "filter": filter,
        }
        return self._h._t.post(f"{self._h._path}/query/near-vector", json=payload).json()

    def fetch_object_by_id(self, uuid: str, *, include_vector: bool | list[str] = False) -> Dict[str, Any]:
        params = {"include_vector": bool(include_vector)}
        return self._h._t.get(f"{self._h._path}/query/object/{_q(uuid)}", params=params).json()

    def fetch_objects(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        include_vector: bool = False,
        include_properties: bool = True,
        newest_first: bool = True,
    ) -> Dict[str, Any]:
        params = {
            "limit": limit,
            "offset": offset,
            "include_vector": include_vector,
            "include_properties": include_properties,
            "newest_first": newest_first,
        }
        return self._h._t.get(f"{self._h._path}/query/objects", params=params).json()

    def iterator(
        self,
        *,
        page_size: int = 500,
        include_vector: bool = False,
        include_properties: bool = True,
        newest_first: bool = True,
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            page = self.fetch_objects(
                limit=page_size,
                offset=offset,
                include_vector=include_vector,
                include_properties=include_properties,
                newest_first=newest_first,
            )
            objs = page.get("objects") or []
            if not objs:
                break
            for obj in objs:
                yield obj
            offset = int(page.get("next_offset") or 0)


class CollectionConfig:
    def __init__(self, handle: "CollectionHandle"):
        self._h = handle

    def _config(self) -> dict[str, Any]:
        return self._h._t.get(f"{self._h._path}/config").json()

    def dims(self) -> int:
        return int(self._config().get("dims", 0))

    def schema(self) -> Dict[str, Any]:
        return dict(self._config().get("schema") or {})

    def shards(self) -> Dict[str, Any]:
        return dict(self._config().get("shards") or {})


class CollectionAdmin:
    def __init__(self, handle: "CollectionHandle"):
        self._h = handle

    def delete_collection(self, *, ensure_remote: bool = True, drop_keys_table: bool = True):
        params = {
            "ensure_remote": ensure_remote,
            "drop_keys_table": drop_keys_table,
        }
        return self._h._t.delete(self._h._path, params=params).json()

    def delete_by_filter(
        self,
        *,
        where: Dict[str, Any],
        limit_object_ids: int | None = None,
        delete_sql_metadata_rows: bool = False,
        on_missing_keys: Literal["skip", "error"] = "skip",
        batch_size: int = 500,
        background: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "where": where,
            "limit_object_ids": limit_object_ids,
            "delete_sql_metadata_rows": delete_sql_metadata_rows,
            "on_missing_keys": on_missing_keys,
            "batch_size": batch_size,
            "background": background,
        }
        return self._h._t.post(f"{self._h._path}/admin/delete-by-filter", json=payload).json()


class CollectionBatch:
    def __init__(self, handle: "CollectionHandle"):
        self._h = handle
        self.failed_objects: List[FailedObject] = []

    def fixed_size(
        self,
        *,
        batch_size: int = 200,
        max_workers: int = 1,
        on_batch_error: Literal["raise", "continue"] = "continue",
    ):
        return _FixedSizeBatchCtx(
            manager=self,
            batch_size=int(batch_size),
            max_workers=int(max_workers),
            on_batch_error=on_batch_error,
        )


class _FixedSizeBatchCtx:
    def __init__(
        self,
        *,
        manager: CollectionBatch,
        batch_size: int,
        max_workers: int,
        on_batch_error: Literal["raise", "continue"],
    ):
        self._m = manager
        self._batch_size = max(1, batch_size)
        self._max_workers = max(1, max_workers)
        self._on_batch_error = on_batch_error

        self._buf: List[dict[str, Any]] = []
        self.failed_objects: List[FailedObject] = []
        self.number_errors: int = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.flush()
        self._m.failed_objects = list(self.failed_objects)
        return False

    def add_object(
        self,
        *,
        properties: Dict[str, Any],
        vector: List[float],
        uuid: Optional[str] = None,
    ) -> str:
        out_uuid = str(uuid) if uuid is not None else str(uuid4())
        self._buf.append(
            {
                "properties": properties,
                "vector": vector,
                "uuid": out_uuid,
            }
        )
        if len(self._buf) >= self._batch_size:
            self.flush()
        return out_uuid

    def flush(self) -> None:
        if not self._buf:
            return

        payload = list(self._buf)
        self._buf = []

        try:
            result = self._m._h.data.insert_many(
                payload,
                on_duplicate="error",
                on_error="continue",
                batch_size=self._batch_size,
                max_workers=self._max_workers,
            )
            self.number_errors += result.number_errors
            self.failed_objects.extend(result.failed_objects)
        except Exception as exc:
            self.number_errors += len(payload)
            self.failed_objects.extend(
                [FailedObject(uuid=str(obj.get("uuid") or ""), error=str(exc)) for obj in payload]
            )
            if self._on_batch_error == "raise":
                raise


class CollectionHandle:
    def __init__(self, transport: Transport, bucket_name: str, name: str):
        self._t = transport
        self._path = _collection_path(bucket_name, name)

        self.data = CollectionData(self)
        self.query = CollectionQuery(self)
        self.config = CollectionConfig(self)
        self.admin = CollectionAdmin(self)
        self.batch = CollectionBatch(self)


class CollectionsNamespace:
    def __init__(self, transport: Transport):
        self._t = transport

    def get_or_create(
        self,
        *,
        bucket_name: str,
        name: str,
        dims: int,
        schema_json: Dict[str, Any] | None = None,
        is_global: bool | None = None,
        **kwargs: Any,
    ) -> CollectionHandle:
        payload = {
            "bucket_name": bucket_name,
            "name": name,
            "dims": int(dims),
            "schema_json": schema_json,
            "is_global": is_global,
            **kwargs,
        }
        self._t.post("/v1/collections/get-or-create", json=payload)
        return CollectionHandle(self._t, bucket_name, name)

    def get(self, *, bucket_name: str, name: str) -> CollectionHandle:
        self._t.get(_collection_path(bucket_name, name))
        return CollectionHandle(self._t, bucket_name, name)

    def use(self, *, bucket_name: str, name: str) -> CollectionHandle:
        return CollectionHandle(self._t, bucket_name, name)


class Client:
    def __init__(
        self,
        *,
        cluster_url: str,
        auth_credentials: ApiKeyAuth | None = None,
        timeout: float = 20.0,
        retries: int = 2,
    ):
        self._transport = Transport(
            base_url=cluster_url,
            auth_credentials=auth_credentials,
            timeout=timeout,
            retries=retries,
        )
        self.collections = CollectionsNamespace(self._transport)

    def is_ready(self) -> bool:
        try:
            payload = self._transport.get("/v1/health/ready").json()
            return bool(payload.get("ready"))
        except Exception:
            return False

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
