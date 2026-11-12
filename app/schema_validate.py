"""Whitelist validation of parsed Avro schemas.

The service deliberately supports only: null, boolean, int, long, string,
record, array and union (plus named-type references, which resolve to one of
those). Logical types, bytes/float/double/enum/fixed/map and any other Avro
type are rejected here with an exact schema path, before any datum is read.
"""
from __future__ import annotations

from typing import Optional

import avro.schema as avsc

from .errors import UnsupportedTypeError

#: Writer/reader types the service is willing to handle.
SUPPORTED_TYPES = frozenset(
    {"null", "boolean", "int", "long", "string", "record", "array", "union"}
)


def _logical_type(schema: avsc.Schema) -> Optional[str]:
    return getattr(schema, "logical_type", None)


def validate_supported(schema: avsc.Schema, side: str, path: str = "$") -> None:
    """Recursively assert that ``schema`` only uses supported constructs.

    ``side`` is ``"writer"`` or ``"reader"`` and only affects error messages.

    Recursive named references reuse the same Python schema object; guarding on
    object identity keeps recursion from looping forever while still validating
    every distinct type exactly once.
    """
    seen: set[int] = set()

    def walk(node: avsc.Schema, node_path: str) -> None:
        if id(node) in seen:
            return
        seen.add(id(node))

        logical = _logical_type(node)
        if logical is not None:
            raise UnsupportedTypeError(
                f"Logical types are not supported: schema at {node_path} "
                f"declares logicalType '{logical}'.",
                **{f"{side}_path": node_path},
            )

        node_type = node.type
        if node_type not in SUPPORTED_TYPES:
            raise UnsupportedTypeError(
                f"Unsupported Avro type '{node_type}' at {node_path}; supported "
                "types are null, boolean, int, long, string, record, array and union.",
                **{f"{side}_path": node_path},
            )

        if isinstance(node, avsc.RecordSchema):
            for index, field in enumerate(node.fields):
                field_path = f"{node_path}.fields[{index}]({field.name})"
                walk(field.type, field_path)
        elif isinstance(node, avsc.ArraySchema):
            walk(node.items, f"{node_path}.items")
        elif isinstance(node, avsc.UnionSchema):
            for index, branch in enumerate(node.schemas):
                walk(branch, f"{node_path}[{index}]")
        # primitives have nothing to descend into

    walk(schema, path)
