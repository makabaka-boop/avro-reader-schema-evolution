"""Focused unit tests for resolution rules that need custom in-memory files."""
from __future__ import annotations

import io
import json

import pytest

from app.avro_json import to_jsonable
from app.container import convert_container, parse_schema
from app.errors import (
    AmbiguousAliasError,
    IncompatibleSchemaError,
    MissingDefaultError,
    UnsupportedTypeError,
)
from tests.test_conversion import _ocf_bytes
import avro.schema as avsc


def _parse(text: str) -> avsc.Schema:
    return avsc.parse(text)


def test_record_alias_and_namespace_qualified_alias() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "legacy.Widget",
                "fields": [{"name": "size", "type": "int"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"size": 4}])
    # Reader lives in a new namespace and reaches the old name via full alias.
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "shop.Gadget",
                "aliases": ["legacy.Widget"],
                "fields": [
                    {"name": "size", "type": "long"},
                    {"name": "label", "type": "string", "default": "none"},
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out == [{"size": 4, "label": "none"}]


def test_unqualified_field_alias_uses_reader_record_namespace() -> None:
    # The bare alias "old_name" must be qualified with the reader record's
    # namespace when compared, but still match the writer's bare field name.
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "ns.Entity",
                "fields": [{"name": "old_name", "type": "string"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"old_name": "hello"}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "ns.Entity",
                "fields": [
                    {"name": "new_name", "type": "string", "aliases": ["old_name"]}
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out == [{"new_name": "hello"}]


def test_record_name_mismatch_without_alias_fails() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "a.One",
                "fields": [{"name": "f", "type": "int"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"f": 1}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "b.Two",
                "fields": [{"name": "f", "type": "int"}],
            }
        ),
        "reader",
    )
    with pytest.raises(IncompatibleSchemaError) as exc:
        convert_container(io.BytesIO(raw), reader)
    assert "alias" in exc.value.message or "mismatch" in exc.value.message


def test_nested_record_default_fully_materialised() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"id": 1}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "meta",
                        "type": {
                            "type": "record",
                            "name": "Meta",
                            "fields": [
                                {"name": "flag", "type": "boolean", "default": True},
                                {
                                    "name": "vals",
                                    "type": {"type": "array", "items": "string"},
                                    "default": ["x"],
                                },
                            ],
                        },
                        "default": {"flag": False, "vals": []},
                    },
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out[0]["meta"] == {"flag": False, "vals": []}


def test_union_default_targets_first_branch() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"id": 1}])
    # First branch is null, so the default is a bare null and must come out
    # tagged as the null branch in Avro JSON.
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "maybe",
                        "type": ["null", "string"],
                        "default": None,
                    },
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out[0]["maybe"] == {"null": None}


def test_two_reader_fields_matching_one_writer_field_is_ambiguous() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "shared", "type": "int"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"shared": 1}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "a", "type": "int", "aliases": ["shared"]},
                    {"name": "b", "type": "long", "aliases": ["shared"]},
                ],
            }
        ),
        "reader",
    )
    with pytest.raises(AmbiguousAliasError):
        convert_container(io.BytesIO(raw), reader)


def test_extra_writer_fields_are_skipped_not_errors() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "keep", "type": "int"},
                    {"name": "drop_me", "type": "string"},
                    {"name": "also_drop", "type": "boolean"},
                ],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"keep": 1, "drop_me": "x", "also_drop": True}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "keep", "type": "long"}],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out == [{"keep": 1}]


def test_boolean_cannot_promote_to_long() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "flag", "type": "boolean"}],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"flag": True}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "flag", "type": "long"}],
            }
        ),
        "reader",
    )
    with pytest.raises(IncompatibleSchemaError):
        convert_container(io.BytesIO(raw), reader)


def test_array_item_promotion_int_to_long() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "nums", "type": {"type": "array", "items": "int"}}
                ],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"nums": [1, 2, 3]}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "nums", "type": {"type": "array", "items": "long"}}
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw), reader))
    assert out == [{"nums": [1, 2, 3]}]


def test_writer_unsupported_type_rejected_with_path() -> None:
    # Container whose writer schema contains a map: reader validation passes,
    # the writer-side whitelist must reject it with a precise path.
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "m", "type": {"type": "map", "values": "string"}}
                ],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"m": {"k": "v"}}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "m", "type": "string"}],
            }
        ),
        "reader",
    )
    with pytest.raises(UnsupportedTypeError) as exc:
        convert_container(io.BytesIO(raw), reader)
    assert exc.value.writer_path == "$.fields[0](m)"


def test_malformed_reader_schema_json_reports_error() -> None:
    from app.errors import ConversionError

    with pytest.raises(ConversionError):
        parse_schema("{not valid json", "reader")


def test_missing_default_inside_nested_record_path() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "Outer",
                "fields": [
                    {
                        "name": "inner",
                        "type": {
                            "type": "record",
                            "name": "Inner",
                            "fields": [{"name": "present", "type": "int"}],
                        },
                    }
                ],
            }
        )
    )
    raw = _ocf_bytes(writer, [{"inner": {"present": 1}}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "Outer",
                "fields": [
                    {
                        "name": "inner",
                        "type": {
                            "type": "record",
                            "name": "Inner",
                            "fields": [
                                {"name": "present", "type": "int"},
                                {"name": "absent", "type": "int"},
                            ],
                        },
                    }
                ],
            }
        ),
        "reader",
    )
    with pytest.raises(MissingDefaultError) as exc:
        convert_container(io.BytesIO(raw), reader)
    assert "absent" in exc.value.reader_path


def test_nested_union_incompatibility_carries_branch_path() -> None:
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {
                        "name": "box",
                        "type": {"type": "array", "items": ["null", "int", "string"]},
                    }
                ],
            }
        )
    )
    # The null writer branch (index 0) has no home in the reader union.
    raw = _ocf_bytes(writer, [{"box": [1, "x", None]}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "box", "type": {"type": "array", "items": ["int"]}}
                ],
            }
        ),
        "reader",
    )
    with pytest.raises(IncompatibleSchemaError) as exc:
        convert_container(io.BytesIO(raw), reader)
    assert exc.value.writer_path == "$.box.items[0]"
    assert "null" in exc.value.message


def test_writer_union_to_non_union_reader_unwraps_actual_branch() -> None:
    # Writer [null, int] read through a plain long reader field. Pre-flight
    # resolution checks *every* union branch against the reader, so the null
    # branch makes the whole request incompatible regardless of the data:
    # the service must fail closed instead of guessing.
    writer = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "n", "type": ["null", "int"], "default": None}],
            }
        )
    )
    # A fully compatible shape first: [int] union -> long reader.
    writer_int_only = _parse(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "n", "type": "int"}],
            }
        )
    )
    raw_ok = _ocf_bytes(writer_int_only, [{"n": 7}])
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "n", "type": "long"}],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(io.BytesIO(raw_ok), reader))
    assert out == [{"n": 7}]

    raw_bad = _ocf_bytes(writer, [{"n": 7}, {"n": None}])
    with pytest.raises(IncompatibleSchemaError) as exc:
        convert_container(io.BytesIO(raw_bad), reader)
    assert "null" in exc.value.message
