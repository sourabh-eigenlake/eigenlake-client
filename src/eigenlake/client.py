from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Literal
from urllib.parse import quote
from uuid import uuid4

from .transport import Transport


def _q(value: str) -> str:
    return quote(str(value), safe="")


def _index_path(namespace: str, index: str) -> str:
    return f"/v1/collections/{_q(namespace)}/{_q(index)}"


@dataclass
class FailedRecord:
    id: str
    error: str


class AddManyResult(list[str]):
    def __init__(self, ids: Iterable[str], *, failed_records: List[FailedRecord] | None = None):
        super().__init__(ids)
        self.failed_records: List[FailedRecord] = failed_records or []

    @property
    def number_errors(self) -> int:
        return len(self.failed_records)


class IndexRecords:
    def __init__(self, handle: "IndexHandle"):
        self._h = handle

    def add(
        self,
        *,
        properties: Dict[str, Any],
        vector: List[float],
        id: str | None = None,
        on_duplicate: Literal["error", "replace", "skip"] = "error",
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> str:
        payload = {
            "properties": properties,
            "vector": vector,
            "uuid": id,
            "on_duplicate": on_duplicate,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        resp = self._h._t.post(f"{self._h._path}/data/insert", json=payload).json()
        return str(resp["uuid"])

    @staticmethod
    def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(record)
        if "id" in item:
            item["uuid"] = item.pop("id")
        return item

    @staticmethod
    def _normalize_vector_item(item: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(item)
        if "id" in normalized:
            normalized["uuid"] = normalized.pop("id")
        return normalized

    def add_many(
        self,
        records: Iterable[dict[str, Any]],
        *,
        on_duplicate: Literal["error", "replace", "skip"] = "error",
        on_error: Literal["raise", "continue"] = "raise",
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> AddManyResult:
        payload = {
            "objects": [self._normalize_record(record) for record in records],
            "on_duplicate": on_duplicate,
            "on_error": on_error,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        resp = self._h._t.post(f"{self._h._path}/data/insert-many", json=payload).json()
        failed = [
            FailedRecord(id=str(item.get("uuid") or ""), error=str(item.get("error") or ""))
            for item in (resp.get("failed_objects") or [])
        ]
        return AddManyResult(resp.get("uuids") or [], failed_records=failed)

    def add_vectors(
        self,
        vectors: Iterable[dict[str, Any]],
        *,
        batch_size: int = 500,
        max_workers: int = 1,
    ) -> None:
        payload = {
            "vectors": [self._normalize_vector_item(item) for item in vectors],
            "batch_size": batch_size,
            "max_workers": max_workers,
        }
        self._h._t.post(f"{self._h._path}/data/insert-vectors", json=payload)

    def get(self, id: str, *, return_data: bool = True, return_metadata: bool = True) -> dict[str, Any] | None:
        payload = {
            "uuid": id,
            "return_data": return_data,
            "return_metadata": return_metadata,
        }
        resp = self._h._t.post(f"{self._h._path}/data/get-by-id", json=payload).json()
        return resp.get("object")

    def exists(self, id: str) -> bool:
        resp = self._h._t.get(f"{self._h._path}/data/exists/{_q(id)}").json()
        return bool(resp.get("exists", False))

    def remove(self, id: str, *, batch_size: int = 500) -> None:
        self._h._t.delete(f"{self._h._path}/data/{_q(id)}", params={"batch_size": batch_size})

    def remove_many(
        self,
        *,
        filter: Dict[str, Any],
        limit: int | None = None,
        delete_sql_rows: bool = False,
        on_missing: Literal["skip", "error"] = "skip",
        batch_size: int = 500,
        background: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "where": filter,
            "limit": limit,
            "delete_patent_rows": delete_sql_rows,
            "on_missing_keys": on_missing,
            "batch_size": batch_size,
            "background": background,
        }
        return self._h._t.post(f"{self._h._path}/data/delete-many", json=payload).json()

    def remove_job(self, job_id: int) -> Dict[str, Any]:
        return self._h._t.get(f"{self._h._path}/data/delete-jobs/{int(job_id)}").json()

    def update(
        self,
        *,
        id: str,
        properties: Dict[str, Any] | None = None,
        vector: List[float] | None = None,
    ) -> None:
        payload = {
            "properties": properties,
            "vector": vector,
        }
        self._h._t.patch(f"{self._h._path}/data/{_q(id)}", json=payload)

    def replace(
        self,
        *,
        id: str,
        properties: Dict[str, Any],
        vector: List[float] | None = None,
    ) -> None:
        payload = {
            "properties": properties,
            "vector": vector,
        }
        self._h._t.put(f"{self._h._path}/data/{_q(id)}", json=payload)

    def list(
        self,
        *,
        filter: Dict[str, Any],
        limit: int = 100,
        after: str | None = None,
        with_vector: bool = False,
        with_properties: bool = True,
        on_missing: Literal["skip", "error"] = "skip",
    ) -> Dict[str, Any]:
        payload = {
            "where": filter,
            "limit": limit,
            "after": after,
            "include_vector": with_vector,
            "include_properties": with_properties,
            "on_missing_keys": on_missing,
        }
        return self._h._t.post(f"{self._h._path}/data/get-by-filter", json=payload).json()


class IndexSearch:
    def __init__(self, handle: "IndexHandle"):
        self._h = handle

    def nearest(self, *, vector: List[float], limit: int = 10, filter: Dict[str, Any] | None = None):
        payload = {
            "vector": vector,
            "top_k": limit,
            "filter": filter,
        }
        return self._h._t.post(f"{self._h._path}/query/near-vector", json=payload).json()

    def get(self, id: str, *, with_vector: bool = False) -> Dict[str, Any]:
        params = {"include_vector": bool(with_vector)}
        return self._h._t.get(f"{self._h._path}/query/object/{_q(id)}", params=params).json()

    def list(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        with_vector: bool = False,
        with_properties: bool = True,
        newest_first: bool = True,
    ) -> Dict[str, Any]:
        params = {
            "limit": limit,
            "offset": offset,
            "include_vector": with_vector,
            "include_properties": with_properties,
            "newest_first": newest_first,
        }
        return self._h._t.get(f"{self._h._path}/query/objects", params=params).json()

    def iterate(
        self,
        *,
        page_size: int = 500,
        with_vector: bool = False,
        with_properties: bool = True,
        newest_first: bool = True,
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            page = self.list(
                limit=page_size,
                offset=offset,
                with_vector=with_vector,
                with_properties=with_properties,
                newest_first=newest_first,
            )
            objects = page.get("objects") or []
            if not objects:
                break
            for obj in objects:
                yield obj
            offset = int(page.get("next_offset") or 0)


class IndexSettings:
    def __init__(self, handle: "IndexHandle"):
        self._h = handle

    def _read(self) -> dict[str, Any]:
        return self._h._t.get(f"{self._h._path}/config").json()

    def dimensions(self) -> int:
        return int(self._read().get("dims", 0))

    def schema(self) -> Dict[str, Any]:
        return dict(self._read().get("schema") or {})

    def shards(self) -> Dict[str, Any]:
        return dict(self._read().get("shards") or {})


class IndexManage:
    def __init__(self, handle: "IndexHandle"):
        self._h = handle

    def delete(self, *, ensure_remote: bool = True, drop_keys_table: bool = True):
        params = {
            "ensure_remote": ensure_remote,
            "drop_keys_table": drop_keys_table,
        }
        return self._h._t.delete(self._h._path, params=params).json()

    def remove_by_filter(
        self,
        *,
        filter: Dict[str, Any],
        limit_ids: int | None = None,
        delete_sql_rows: bool = False,
        on_missing: Literal["skip", "error"] = "skip",
        batch_size: int = 500,
        background: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "where": filter,
            "limit_object_ids": limit_ids,
            "delete_sql_metadata_rows": delete_sql_rows,
            "on_missing_keys": on_missing,
            "batch_size": batch_size,
            "background": background,
        }
        return self._h._t.post(f"{self._h._path}/admin/delete-by-filter", json=payload).json()


class IndexBatch:
    def __init__(self, handle: "IndexHandle"):
        self._h = handle
        self.failed_records: List[FailedRecord] = []

    def with_size(
        self,
        *,
        batch_size: int = 200,
        max_workers: int = 1,
        on_error: Literal["raise", "continue"] = "continue",
    ):
        return _SizedBatchWriter(
            manager=self,
            batch_size=int(batch_size),
            max_workers=int(max_workers),
            on_error=on_error,
        )


class _SizedBatchWriter:
    def __init__(
        self,
        *,
        manager: IndexBatch,
        batch_size: int,
        max_workers: int,
        on_error: Literal["raise", "continue"],
    ):
        self._m = manager
        self._batch_size = max(1, batch_size)
        self._max_workers = max(1, max_workers)
        self._on_error = on_error

        self._buffer: List[dict[str, Any]] = []
        self.failed_records: List[FailedRecord] = []
        self.number_errors: int = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.flush()
        self._m.failed_records = list(self.failed_records)
        return False

    def add(
        self,
        *,
        properties: Dict[str, Any],
        vector: List[float],
        id: str | None = None,
    ) -> str:
        out_id = str(id) if id is not None else str(uuid4())
        self._buffer.append(
            {
                "properties": properties,
                "vector": vector,
                "id": out_id,
            }
        )
        if len(self._buffer) >= self._batch_size:
            self.flush()
        return out_id

    def flush(self) -> None:
        if not self._buffer:
            return

        payload = list(self._buffer)
        self._buffer = []

        try:
            result = self._m._h.records.add_many(
                payload,
                on_duplicate="error",
                on_error="continue",
                batch_size=self._batch_size,
                max_workers=self._max_workers,
            )
            self.number_errors += result.number_errors
            self.failed_records.extend(result.failed_records)
        except Exception as exc:
            self.number_errors += len(payload)
            self.failed_records.extend(
                [FailedRecord(id=str(item.get("id") or ""), error=str(exc)) for item in payload]
            )
            if self._on_error == "raise":
                raise


class IndexHandle:
    def __init__(self, transport: Transport, namespace: str, index: str):
        self._t = transport
        self._path = _index_path(namespace, index)

        self.records = IndexRecords(self)
        self.search = IndexSearch(self)
        self.settings = IndexSettings(self)
        self.manage = IndexManage(self)
        self.batch = IndexBatch(self)


class IndexesNamespace:
    def __init__(self, transport: Transport):
        self._t = transport

    def create_or_get(
        self,
        *,
        namespace: str,
        index: str,
        dimensions: int,
        schema: Dict[str, Any] | None = None,
        index_options: Dict[str, Any] | None = None,
        shard_count: int = 1,
        record_id_property: str = "document_id",
    ) -> IndexHandle:
        shard_count = max(1, int(shard_count))
        payload = {
            "namespace": namespace,
            "index": index,
            "dimensions": int(dimensions),
            "schema": schema,
            "index_options": index_options,
            "shard_count": shard_count,
            "record_id_property": record_id_property,
        }
        self._t.post("/v1/collections/get-or-create", json=payload)
        return IndexHandle(self._t, namespace, index)

    def open(self, *, namespace: str, index: str) -> IndexHandle:
        self._t.get(_index_path(namespace, index))
        return IndexHandle(self._t, namespace, index)

    def ref(self, *, namespace: str, index: str) -> IndexHandle:
        return IndexHandle(self._t, namespace, index)


class EigenLakeClient:
    def __init__(
        self,
        *,
        url: str,
        api_key: str | None = None,
        timeout: float = 20.0,
        retries: int = 2,
    ):
        self._transport = Transport(
            base_url=url,
            api_key=api_key,
            timeout=timeout,
            retries=retries,
        )
        self.indexes = IndexesNamespace(self._transport)

    def ready(self) -> bool:
        try:
            payload = self._transport.get("/v1/health/ready").json()
            return bool(payload.get("ready"))
        except Exception:
            return False

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "EigenLakeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
