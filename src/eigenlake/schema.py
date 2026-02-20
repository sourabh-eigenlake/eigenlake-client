from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Field:
    field_type: str
    required: bool = False
    filterable: bool = True
    format: str | None = None
    description: str | None = None
    enum: list[Any] | None = None
    minimum: int | float | None = None
    maximum: int | float | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    items: dict[str, Any] | None = None
    min_items: int | None = None
    max_items: int | None = None
    unique_items: bool = False

    def to_json_schema(self) -> dict[str, Any]:
        out: dict[str, Any] = {"type": self.field_type}
        if self.format is not None:
            out["format"] = self.format
        if self.description is not None:
            out["description"] = self.description
        if self.enum is not None:
            out["enum"] = list(self.enum)
        if self.minimum is not None:
            out["minimum"] = self.minimum
        if self.maximum is not None:
            out["maximum"] = self.maximum
        if self.min_length is not None:
            out["minLength"] = self.min_length
        if self.max_length is not None:
            out["maxLength"] = self.max_length
        if self.pattern is not None:
            out["pattern"] = self.pattern
        if self.items is not None:
            out["items"] = dict(self.items)
        if self.min_items is not None:
            out["minItems"] = self.min_items
        if self.max_items is not None:
            out["maxItems"] = self.max_items
        if self.unique_items:
            out["uniqueItems"] = True
        return out


def string(
    *,
    required: bool = False,
    filterable: bool = True,
    format: str | None = None,
    description: str | None = None,
    enum: list[str] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
) -> Field:
    return Field(
        field_type="string",
        required=required,
        filterable=filterable,
        format=format,
        description=description,
        enum=list(enum) if enum is not None else None,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
    )


def integer(
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> Field:
    return Field(
        field_type="integer",
        required=required,
        filterable=filterable,
        description=description,
        minimum=minimum,
        maximum=maximum,
    )


def number(
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
    minimum: int | float | None = None,
    maximum: int | float | None = None,
) -> Field:
    return Field(
        field_type="number",
        required=required,
        filterable=filterable,
        description=description,
        minimum=minimum,
        maximum=maximum,
    )


def boolean(
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
) -> Field:
    return Field(
        field_type="boolean",
        required=required,
        filterable=filterable,
        description=description,
    )


def array(
    item: Field | dict[str, Any],
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
    min_items: int | None = None,
    max_items: int | None = None,
    unique_items: bool = False,
) -> Field:
    if isinstance(item, Field):
        item_schema = item.to_json_schema()
    else:
        item_schema = dict(item)
    return Field(
        field_type="array",
        required=required,
        filterable=filterable,
        description=description,
        items=item_schema,
        min_items=min_items,
        max_items=max_items,
        unique_items=unique_items,
    )


def datetime(
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
) -> Field:
    return string(
        required=required,
        filterable=filterable,
        format="date-time",
        description=description,
    )


def date(
    *,
    required: bool = False,
    filterable: bool = True,
    description: str | None = None,
) -> Field:
    return string(
        required=required,
        filterable=filterable,
        format="date",
        description=description,
    )


class SchemaBuilder:
    def __init__(self, *, additional_properties: bool = False):
        self._additional_properties = bool(additional_properties)
        self._fields: dict[str, Field] = {}

    def add(self, name: str, field: Field) -> "SchemaBuilder":
        self._fields[str(name)] = field
        return self

    def build(self) -> tuple[dict[str, Any], dict[str, Any]]:
        properties = {name: field.to_json_schema() for name, field in self._fields.items()}
        required = [name for name, field in self._fields.items() if field.required]
        non_filterable = [name for name, field in self._fields.items() if not field.filterable]

        schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": self._additional_properties,
            "properties": properties,
        }
        if required:
            schema["required"] = required

        index_options: dict[str, Any] = {
            "metadataConfiguration": {
                "nonFilterableMetadataKeys": non_filterable,
            }
        }
        return schema, index_options
