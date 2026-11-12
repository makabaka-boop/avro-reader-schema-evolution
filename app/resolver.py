"""Core Avro writer-schema -> reader-schema resolution.

The Avro library is used only for:

* parsing the container file and decompressing its blocks (null/deflate), and
* consuming the writer's binary encoding with the *writer's* schema.

Every resolution rule below is implemented here on purpose. No library decoder
is ever handed the reader schema: the actual union branch selected by the
writer is read first, the matching reader branch is chosen by this module, and
the branch identity is preserved in a :class:`Tagged` value for Avro JSON
encoding.

Supported types: null, boolean, int, long, string, record, array, union.
The only cross-type numeric promotion is int -> long.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import avro.io as avio
import avro.schema as avsc

from .errors import (
    AmbiguousAliasError,
    IncompatibleSchemaError,
    InvalidDefaultError,
    MissingDefaultError,
)

# --------------------------------------------------------------------------- #
# Tagged union representation
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Tagged:
    """A value paired with the Avro JSON union-branch tag.

    ``tag`` is the Avro JSON encoding branch name: the primitive name for
    primitive branches or the record's full name for named branches.
    ``branch_index`` is the position of the branch in its writer union, used
    to build precise schema paths in error messages.
    """

    tag: str
    value: Any
    branch_index: int = -1


# --------------------------------------------------------------------------- #
# Small path helpers
# --------------------------------------------------------------------------- #


def _field_path(base: str, field_name: str) -> str:
    return f"{base}.{field_name}" if base != "$" else f"$.{field_name}"


def _union_path(base: str, index: int) -> str:
    return f"{base}[{index}]"


# --------------------------------------------------------------------------- #
# Alias handling (Avro 1.11.x specification)
# --------------------------------------------------------------------------- #


def _full_aliases(schema_or_field: Any, enclosing_namespace: Optional[str]) -> frozenset[str]:
    """Return the aliases of a record/field, qualified to full names."""
    props = getattr(schema_or_field, "props", {}) or {}
    raw_aliases = props.get("aliases") or []
    qualified: set[str] = set()
    for alias in raw_aliases:
        if "." in alias:
            qualified.add(alias)
        elif enclosing_namespace:
            qualified.add(f"{enclosing_namespace}.{alias}")
        else:
            qualified.add(alias)
    return frozenset(qualified)


def _record_fullname(schema: avsc.RecordSchema) -> str:
    if schema.namespace:
        return f"{schema.namespace}.{schema.name}"
    return schema.name


def _records_match(writer: avsc.RecordSchema, reader: avsc.RecordSchema) -> bool:
    """Record names match when equal, or when the writer name is a reader alias."""
    writer_name = _record_fullname(writer)
    reader_name = _record_fullname(reader)
    if writer_name == reader_name:
        return True
    return writer_name in _full_aliases(reader, reader.namespace)


# --------------------------------------------------------------------------- #
# Compatibility predicates
# --------------------------------------------------------------------------- #


def _primitives_compatible(writer_type: str, reader_type: str) -> bool:
    if writer_type == reader_type:
        return True
    # The single supported promotion: int is readable as long.
    return writer_type == "int" and reader_type == "long"


def _match_branch(
    writer_schema: avsc.Schema, reader_branch: avsc.Schema
) -> bool:
    """Whether one concrete writer schema can be read through one reader schema.

    Name equality for records is enforced (including reader aliases); unions
    inside a branch are handled structurally by the caller.
    """
    # Both unions: every writer branch must find *some* reader branch.
    if isinstance(writer_schema, avsc.UnionSchema) and isinstance(
        reader_branch, avsc.UnionSchema
    ):
        return all(
            _find_reader_branch(wb, reader_branch) is not None
            for wb in writer_schema.schemas
        )
    # Only one side is a union.
    if isinstance(reader_branch, avsc.UnionSchema):
        return _find_reader_branch(writer_schema, reader_branch) is not None
    if isinstance(writer_schema, avsc.UnionSchema):
        return all(
            _match_branch(wb, reader_branch) for wb in writer_schema.schemas
        )

    if isinstance(writer_schema, avsc.RecordSchema) or isinstance(
        reader_branch, avsc.RecordSchema
    ):
        if not (
            isinstance(writer_schema, avsc.RecordSchema)
            and isinstance(reader_branch, avsc.RecordSchema)
        ):
            return False
        return _records_match(writer_schema, reader_branch)

    if isinstance(writer_schema, avsc.ArraySchema) or isinstance(
        reader_branch, avsc.ArraySchema
    ):
        return (
            isinstance(writer_schema, avsc.ArraySchema)
            and isinstance(reader_branch, avsc.ArraySchema)
            and _match_branch(writer_schema.items, reader_branch.items)
        )

    return _primitives_compatible(writer_schema.type, reader_branch.type)


def _find_reader_branch(
    writer_schema: avsc.Schema,
    reader_union: avsc.UnionSchema,
    writer_path: str = "$",
    reader_path: str = "$",
) -> Optional[int]:
    """Return the index of the first compatible reader branch, or None."""
    for index, reader_branch in enumerate(reader_union.schemas):
        if _match_branch(writer_schema, reader_branch):
            return index
    return None


# --------------------------------------------------------------------------- #
# Pre-flight structural compatibility (whole-schema, before reading data)
# --------------------------------------------------------------------------- #


def check_compatible(
    writer: avsc.Schema, reader: avsc.Schema, writer_path: str = "$", reader_path: str = "$"
) -> None:
    """Validate that writer data can be resolved under the reader schema.

    Cycles are handled by memoising on the (writer object, reader object)
    pair, so repeated references are accepted instead of being mistaken for an
    incompatibility, while genuinely recursive structures terminate.
    """
    seen: set[tuple[int, int]] = set()

    def walk(w: avsc.Schema, r: avsc.Schema, wpath: str, rpath: str) -> None:
        key = (id(w), id(r))
        if key in seen:
            return
        seen.add(key)

        # Reader union: the writer (union or not) must resolve per actual branch.
        if isinstance(r, avsc.UnionSchema):
            writer_branches = w.schemas if isinstance(w, avsc.UnionSchema) else [w]
            for w_index, wb in enumerate(writer_branches):
                wp = (
                    _union_path(wpath, w_index)
                    if isinstance(w, avsc.UnionSchema)
                    else wpath
                )
                idx = _find_reader_branch(wb, r, wp, rpath)
                if idx is None:
                    raise IncompatibleSchemaError(
                        "No compatible branch in reader union for writer type "
                        f"'{_describe(wb)}'.",
                        writer_path=wp,
                        reader_path=rpath,
                    )
                walk(wb, r.schemas[idx], wp, _union_path(rpath, idx))
            return

        # Writer union, non-union reader: every branch must fit the reader.
        if isinstance(w, avsc.UnionSchema):
            for w_index, wb in enumerate(w.schemas):
                walk(wb, r, _union_path(wpath, w_index), rpath)
            return

        if isinstance(r, avsc.RecordSchema):
            if not isinstance(w, avsc.RecordSchema) or not _records_match(w, r):
                raise IncompatibleSchemaError(
                    f"Reader expects record '{_record_fullname(r)}' but writer "
                    f"provides '{_describe(w)}' (name/alias mismatch).",
                    writer_path=wpath,
                    reader_path=rpath,
                )
            _check_record_fields(w, r, wpath, rpath)
            return

        if isinstance(r, avsc.ArraySchema):
            if not isinstance(w, avsc.ArraySchema):
                raise IncompatibleSchemaError(
                    f"Reader expects array but writer provides '{_describe(w)}'.",
                    writer_path=wpath,
                    reader_path=rpath,
                )
            walk(w.items, r.items, f"{wpath}.items", f"{rpath}.items")
            return

        # Both primitives at this point.
        if not _primitives_compatible(w.type, r.type):
            raise IncompatibleSchemaError(
                f"Writer type '{w.type}' cannot be read as reader type "
                f"'{r.type}'; only int -> long promotion is supported.",
                writer_path=wpath,
                reader_path=rpath,
            )

    def _check_record_fields(
        w: avsc.RecordSchema, r: avsc.RecordSchema, wpath: str, rpath: str
    ) -> None:
        # Map every writer field name to its schema; consumed names detect the
        # "one writer field matches several reader fields" ambiguity.
        writer_fields = {field.name: field for field in w.fields}
        consumed: set[str] = set()

        for reader_field in r.fields:
            rf_path = _field_path(rpath, reader_field.name)
            writer_field = None

            if reader_field.name in writer_fields:
                writer_field = writer_fields[reader_field.name]
            else:
                matches: list[str] = []
                for alias in _full_aliases(reader_field, r.namespace):
                    # Field aliases are compared against the writer field's
                    # bare name; qualified aliases use their final component.
                    candidate = alias.split(".")[-1]
                    if candidate in writer_fields:
                        matches.append(candidate)
                if len(matches) > 1:
                    raise AmbiguousAliasError(
                        f"Reader field '{reader_field.name}' aliases "
                        f"{sorted(matches)} match multiple writer fields.",
                        writer_path=wpath,
                        reader_path=rf_path,
                        details={"aliases": sorted(matches)},
                    )
                if len(matches) == 1:
                    writer_field = writer_fields[matches[0]]

            if writer_field is not None:
                if writer_field.name in consumed:
                    raise AmbiguousAliasError(
                        f"Writer field '{writer_field.name}' is matched by more "
                        f"than one reader field (last seen by '{reader_field.name}').",
                        writer_path=_field_path(wpath, writer_field.name),
                        reader_path=rf_path,
                    )
                consumed.add(writer_field.name)
                walk(
                    writer_field.type,
                    reader_field.type,
                    _field_path(wpath, writer_field.name),
                    rf_path,
                )
            else:
                # Field deleted/renamed away: a reader default is mandatory.
                if not reader_field.has_default:
                    raise MissingDefaultError(
                        f"Reader field '{reader_field.name}' has no matching "
                        "writer field and declares no default.",
                        writer_path=wpath,
                        reader_path=rf_path,
                    )
                # Validate the default eagerly so a bad default fails the
                # whole request rather than a single record later on.
                build_default(reader_field, r.namespace, rf_path)

    walk(writer, reader, writer_path, reader_path)


def _describe(schema: avsc.Schema) -> str:
    if isinstance(schema, avsc.RecordSchema):
        return f"record {_record_fullname(schema)}"
    if isinstance(schema, avsc.ArraySchema):
        return "array"
    if isinstance(schema, avsc.UnionSchema):
        return "union[" + ", ".join(_describe(b) for b in schema.schemas) + "]"
    return schema.type


# --------------------------------------------------------------------------- #
# Default-value construction
# --------------------------------------------------------------------------- #


def build_default(field: avsc.Field, enclosing_namespace: Optional[str], path: str) -> Any:
    """Interpret a field's JSON default under its (reader) schema.

    Union defaults must target the first branch, per the Avro specification.
    Defaults are returned already in the internal tagged representation so
    that downstream encoding is uniform.
    """
    return _build_default_value(field.type, field.default, path, enclosing_namespace)


def _build_default_value(
    schema: avsc.Schema, raw_default: Any, path: str, namespace: Optional[str]
) -> Any:
    if isinstance(schema, avsc.UnionSchema):
        # Per the Avro spec, a union default is validated against the *first*
        # branch directly (e.g. ["null","X"] defaults to bare null, not
        # {"null": null}). We still wrap the result in Tagged so downstream
        # union branch identity is preserved.
        first = schema.schemas[0]
        nested = _build_default_value(first, raw_default, path, namespace)
        return Tagged(_branch_tag(first), nested)

    if isinstance(schema, avsc.RecordSchema):
        if not isinstance(raw_default, dict):
            raise InvalidDefaultError(
                f"Default for record '{_record_fullname(schema)}' must be a JSON object.",
                reader_path=path,
            )
        result: dict[str, Any] = {}
        for sub_field in schema.fields:
            sub_path = _field_path(path, sub_field.name)
            if sub_field.name in raw_default:
                result[sub_field.name] = _build_default_value(
                    sub_field.type, raw_default[sub_field.name], sub_path, schema.namespace
                )
            elif sub_field.has_default:
                result[sub_field.name] = _build_default_value(
                    sub_field.type, sub_field.default, sub_path, schema.namespace
                )
            else:
                raise MissingDefaultError(
                    f"Default object for record is missing field '{sub_field.name}'.",
                    reader_path=sub_path,
                )
        return result

    if isinstance(schema, avsc.ArraySchema):
        if not isinstance(raw_default, list):
            raise InvalidDefaultError(
                "Default for an array field must be a JSON array.", reader_path=path
            )
        return [
            _build_default_value(schema.items, item, f"{path}[{i}]", namespace)
            for i, item in enumerate(raw_default)
        ]

    # Primitives.
    type_ = schema.type
    if type_ == "null":
        if raw_default is not None:
            raise InvalidDefaultError(
                f"Default for null field must be null, got {raw_default!r}.",
                reader_path=path,
            )
        return None
    if type_ == "boolean":
        if not isinstance(raw_default, bool):
            raise InvalidDefaultError(
                f"Default for boolean field must be true/false, got {raw_default!r}.",
                reader_path=path,
            )
        return raw_default
    if type_ in ("int", "long"):
        if isinstance(raw_default, bool) or not isinstance(raw_default, int):
            raise InvalidDefaultError(
                f"Default for {type_} field must be an integer, got {raw_default!r}.",
                reader_path=path,
            )
        return int(raw_default)
    if type_ == "string":
        if not isinstance(raw_default, str):
            raise InvalidDefaultError(
                f"Default for string field must be a string, got {raw_default!r}.",
                reader_path=path,
            )
        return raw_default

    raise InvalidDefaultError(
        f"Cannot build default for unsupported type '{type_}'.", reader_path=path
    )


# --------------------------------------------------------------------------- #
# Binary reading with the *writer's* schema, keeping union branch identity
# --------------------------------------------------------------------------- #


def read_writer_value(schema: avsc.Schema, decoder: avio.BinaryDecoder) -> Any:
    """Read one datum strictly per the writer schema.

    Unlike the library's plain DatumReader, union values are returned as
    :class:`Tagged` so the actual branch chosen by the writer survives.
    """
    if isinstance(schema, avsc.UnionSchema):
        branch_index = decoder.read_long()
        if branch_index < 0 or branch_index >= len(schema.schemas):
            raise IncompatibleSchemaError(
                f"Union branch index {branch_index} is out of range "
                f"(0..{len(schema.schemas) - 1}).",
            )
        branch = schema.schemas[branch_index]
        return Tagged(
            _branch_tag(branch),
            read_writer_value(branch, decoder),
            branch_index=branch_index,
        )

    if isinstance(schema, avsc.RecordSchema):
        record: dict[str, Any] = {}
        for field in schema.fields:
            record[field.name] = read_writer_value(field.type, decoder)
        return record

    if isinstance(schema, avsc.ArraySchema):
        items: list[Any] = []
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                decoder.read_long()  # block byte size, not needed here
            for _ in range(block_count):
                items.append(read_writer_value(schema.items, decoder))
            block_count = decoder.read_long()
        return items

    type_ = schema.type
    if type_ == "null":
        decoder.read_null()
        return None
    if type_ == "boolean":
        return decoder.read_boolean()
    if type_ == "int":
        return decoder.read_int()
    if type_ == "long":
        return decoder.read_long()
    if type_ == "string":
        return decoder.read_utf8()
    raise IncompatibleSchemaError(
        f"Reader reached unsupported writer type '{type_}' while decoding."
    )


def _branch_tag(schema: avsc.Schema) -> str:
    if isinstance(schema, avsc.RecordSchema):
        return _record_fullname(schema)
    return schema.type


# --------------------------------------------------------------------------- #
# Resolution: convert a writer-tagged value into the reader representation
# --------------------------------------------------------------------------- #


def resolve_value(
    writer: avsc.Schema,
    reader: avsc.Schema,
    value: Any,
    *,
    writer_path: str,
    reader_path: str,
    record_index: Optional[int] = None,
) -> Any:
    """Project a value read under the writer schema onto the reader schema."""
    # Reader union: pick the first compatible branch using the *actual* writer
    # branch when the value came from a writer union.
    if isinstance(reader, avsc.UnionSchema):
        if isinstance(value, Tagged):
            branch_writer_path = (
                _union_path(writer_path, value.branch_index)
                if isinstance(writer, avsc.UnionSchema) and value.branch_index >= 0
                else writer_path
            )
            writer_branch = _writer_branch_for_tag(writer, value.tag, branch_writer_path)
            idx = _find_reader_branch(writer_branch, reader, branch_writer_path, reader_path)
            if idx is None:
                raise IncompatibleSchemaError(
                    f"Writer union branch '{value.tag}' has no compatible "
                    "reader union branch.",
                    writer_path=branch_writer_path,
                    reader_path=reader_path,
                    record_index=record_index,
                )
            inner = resolve_value(
                writer_branch,
                reader.schemas[idx],
                value.value,
                writer_path=branch_writer_path,
                reader_path=_union_path(reader_path, idx),
                record_index=record_index,
            )
            return Tagged(_branch_tag(reader.schemas[idx]), inner)

        idx = _find_reader_branch(writer, reader, writer_path, reader_path)
        if idx is None:
            raise IncompatibleSchemaError(
                f"Writer type '{_describe(writer)}' has no compatible reader "
                "union branch.",
                writer_path=writer_path,
                reader_path=reader_path,
                record_index=record_index,
            )
        inner = resolve_value(
            writer, reader.schemas[idx], value,
            writer_path=writer_path,
            reader_path=_union_path(reader_path, idx),
            record_index=record_index,
        )
        return Tagged(_branch_tag(reader.schemas[idx]), inner)

    # Writer union, non-union reader: unwrap the actual branch first.
    if isinstance(writer, avsc.UnionSchema):
        if not isinstance(value, Tagged):
            raise IncompatibleSchemaError(
                "Expected a tagged writer union value.",
                writer_path=writer_path,
                reader_path=reader_path,
                record_index=record_index,
            )
        branch_writer_path = (
            _union_path(writer_path, value.branch_index)
            if value.branch_index >= 0
            else writer_path
        )
        writer_branch = _writer_branch_for_tag(writer, value.tag, branch_writer_path)
        return resolve_value(
            writer_branch, reader, value.value,
            writer_path=branch_writer_path,
            reader_path=reader_path,
            record_index=record_index,
        )

    if isinstance(reader, avsc.RecordSchema):
        return _resolve_record(writer, reader, value, writer_path, reader_path, record_index)

    if isinstance(reader, avsc.ArraySchema):
        if not isinstance(value, list):
            raise IncompatibleSchemaError(
                "Expected an array value.",
                writer_path=writer_path,
                reader_path=reader_path,
                record_index=record_index,
            )
        return [
            resolve_value(
                writer.items, reader.items, item,
                writer_path=f"{writer_path}.items",
                reader_path=f"{reader_path}.items",
                record_index=record_index,
            )
            for item in value
        ]

    # Primitives. The only promotion is int -> long; the Python int produced by
    # the decoder already represents both, so an explicit check suffices.
    if writer.type == reader.type:
        return value
    if writer.type == "int" and reader.type == "long":
        return int(value)
    raise IncompatibleSchemaError(
        f"Writer type '{writer.type}' cannot be promoted to reader type "
        f"'{reader.type}'.",
        writer_path=writer_path,
        reader_path=reader_path,
        record_index=record_index,
    )


def _writer_branch_for_tag(
    writer_union: avsc.Schema, tag: str, writer_path: str
) -> avsc.Schema:
    if not isinstance(writer_union, avsc.UnionSchema):
        raise IncompatibleSchemaError(
            f"Tagged union value '{tag}' encountered under a non-union "
            "writer schema.",
            writer_path=writer_path,
        )
    for branch in writer_union.schemas:
        if _branch_tag(branch) == tag:
            return branch
    raise IncompatibleSchemaError(
        f"Union tag '{tag}' does not name any branch of the writer union.",
        writer_path=writer_path,
    )


def _resolve_record(
    writer: avsc.RecordSchema,
    reader: avsc.RecordSchema,
    value: dict[str, Any],
    writer_path: str,
    reader_path: str,
    record_index: Optional[int],
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise IncompatibleSchemaError(
            "Expected a record value.",
            writer_path=writer_path,
            reader_path=reader_path,
            record_index=record_index,
        )

    writer_fields = {field.name: field for field in writer.fields}
    result: dict[str, Any] = {}
    consumed: set[str] = set()

    for reader_field in reader.fields:
        rf_path = _field_path(reader_path, reader_field.name)
        writer_field = None
        if reader_field.name in writer_fields:
            writer_field = writer_fields[reader_field.name]
        else:
            matches: list[str] = []
            for alias in _full_aliases(reader_field, reader.namespace):
                candidate = alias.split(".")[-1]
                if candidate in writer_fields:
                    matches.append(candidate)
            if len(matches) > 1:
                raise AmbiguousAliasError(
                    f"Reader field '{reader_field.name}' aliases "
                    f"{sorted(matches)} match multiple writer fields.",
                    writer_path=writer_path,
                    reader_path=rf_path,
                    record_index=record_index,
                )
            if len(matches) == 1:
                writer_field = writer_fields[matches[0]]

        if writer_field is None:
            # Deleted/renamed field: fill from the reader default.
            result[reader_field.name] = build_default(reader_field, reader.namespace, rf_path)
            continue

        if writer_field.name in consumed:
            raise AmbiguousAliasError(
                f"Writer field '{writer_field.name}' is matched by more than "
                f"one reader field (last seen by '{reader_field.name}').",
                writer_path=_field_path(writer_path, writer_field.name),
                reader_path=rf_path,
                record_index=record_index,
            )
        consumed.add(writer_field.name)

        raw = value[writer_field.name]
        # An explicit writer null is a real value: it must never be replaced by
        # a reader default. It flows through normal resolution (and will fail if
        # the reader field is not null-compatible).
        result[reader_field.name] = resolve_value(
            writer_field.type,
            reader_field.type,
            raw,
            writer_path=_field_path(writer_path, writer_field.name),
            reader_path=rf_path,
            record_index=record_index,
        )

    return result
