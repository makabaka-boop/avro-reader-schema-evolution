"""Avro JSON encoding of internally resolved values.

Output follows the Avro 1.11.x JSON encoding rules:

* primitives, records and arrays encode as their natural JSON forms;
* a union value encodes as ``{"branch-tag": value}`` where the tag is the
  primitive type name (``"null"``, ``"string"`` ...) or the record full name,
  preserving which branch the writer actually selected (mapped onto the reader
  branch chosen during resolution);
* a union null branch therefore appears as ``{"null": null}`` rather than a
  bare ``null``, keeping branch identity visible.
"""
from __future__ import annotations

from typing import Any

from .resolver import Tagged


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Tagged):
        return {value.tag: to_jsonable(value.value)}
    if isinstance(value, dict):
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    return value
