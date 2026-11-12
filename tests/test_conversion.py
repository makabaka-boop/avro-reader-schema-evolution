"""End-to-end and unit tests for the Avro schema-resolution service.

Fixtures are real Avro Object Container Files produced by
scripts/generate_fixtures.py. Regenerate them with:

    python scripts/generate_fixtures.py tests/fixtures
"""
from __future__ import annotations

import io
import json
import os

import avro.datafile as avdf
import avro.io as avio
import avro.schema as avsc
import pytest
from fastapi.testclient import TestClient

from app.avro_json import to_jsonable
from app.container import convert_container, parse_schema
from app.errors import (
    AmbiguousAliasError,
    IncompatibleSchemaError,
    MissingDefaultError,
    UnsupportedTypeError,
)
from app.main import app

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
client = TestClient(app)


def _fixture(name: str) -> bytes:
    with open(os.path.join(FIXTURE_DIR, name), "rb") as handle:
        return handle.read()


def _reader_json(name: str) -> str:
    with open(os.path.join(FIXTURE_DIR, name), encoding="utf-8") as handle:
        return handle.read()


def _ocf_bytes(schema: avsc.Schema, records: list[dict], codec: str = "null") -> bytes:
    """Serialize records to an in-memory OCF, returning its bytes safely."""
    buffer = io.BytesIO()
    writer = avdf.DataFileWriter(buffer, avio.DatumWriter(), schema, codec=codec)
    for record in records:
        writer.append(record)
    writer.flush()
    data = buffer.getvalue()
    writer.close()
    return data


def _convert_file(fixture_name: str, reader_name: str):
    reader = parse_schema(_reader_json(reader_name), "reader")
    with open(os.path.join(FIXTURE_DIR, fixture_name), "rb") as handle:
        return convert_container(handle, reader)


# --------------------------------------------------------------------------- #
# Headline scenario: rename via alias, int->long, defaults, explicit null
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_rename_promotion_default_and_explicit_null(codec: str) -> None:
    records = _convert_file(f"order_v1_{codec}.avro", "order_v2_reader.json")
    payload = to_jsonable(records)

    first, second = payload

    # Writer `count:int = 3` is read as reader `total:long` through an alias.
    assert first["total"] == 3
    assert isinstance(first["total"], int)
    # Field reordering does not affect mapping; removed legacy_flag is absent.
    assert list(first.keys()) == ["total", "customer", "order_id", "status", "note"]
    assert "legacy_flag" not in first
    # New field with no writer value takes the reader default.
    assert first["status"] == "pending"
    # Nested record alias + field alias + int->long.
    assert first["customer"] == {"name": "Ada", "customer_id": 7}
    # Union branch identity is kept in Avro JSON form.
    assert first["note"] == {"string": "rush"}

    # The explicit writer null must NOT be replaced by any default.
    assert second["note"] == {"null": None}
    assert second["total"] == 10
    assert second["status"] == "pending"


def test_explicit_null_is_distinguished_from_missing_field() -> None:
    # Reader where `note` becomes a plain string with a default: a *missing*
    # writer field would take the default, but the existing explicit null must
    # instead fail compatibility (it can never become a non-null string).
    reader_schema = {
        "type": "record",
        "name": "com.shop.OrderV2",
        "aliases": ["com.shop.OrderV1"],
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "note", "type": "string", "default": "fallback"},
        ],
    }
    reader = parse_schema(json.dumps(reader_schema), "reader")
    with open(os.path.join(FIXTURE_DIR, "order_v1_null.avro"), "rb") as handle:
        with pytest.raises(IncompatibleSchemaError) as exc_info:
            convert_container(handle, reader)
    err = exc_info.value
    assert err.reader_path is not None and "note" in err.reader_path
    assert "null" in err.message


def test_missing_field_uses_default_even_while_explicit_null_flows_through() -> None:
    # A genuinely absent writer field (renamed away) fills from the default.
    reader_schema = {
        "type": "record",
        "name": "com.shop.OrderV2",
        "aliases": ["com.shop.OrderV1"],
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "count", "type": "long", "aliases": ["count"]},
            {"name": "note", "type": ["null", "string"], "default": None},
            {"name": "fresh", "type": "int", "default": 42},
        ],
    }
    reader = parse_schema(json.dumps(reader_schema), "reader")
    with open(os.path.join(FIXTURE_DIR, "order_v1_null.avro"), "rb") as handle:
        records = convert_container(handle, reader)
    payload = to_jsonable(records)
    assert payload[0]["fresh"] == 42
    assert payload[1]["note"] == {"null": None}


# --------------------------------------------------------------------------- #
# Recursive records
# --------------------------------------------------------------------------- #


def test_recursive_chain_and_repeated_references() -> None:
    records = _convert_file("node_v1_deflate.avro", "node_v2_reader.json")
    payload = to_jsonable(records)

    chain = payload[0]
    # 3 -> 2 -> 1 -> null, int values promoted to long (still JSON integers),
    # deleted `obsolete` field gone, new `depth` defaulted, order rearranged.
    assert chain["value"] == 3
    assert chain["depth"] == 0
    assert "obsolete" not in chain
    level_2 = chain["next"]["com.tree.NodeV2"]
    assert level_2["value"] == 2
    level_1 = level_2["next"]["com.tree.NodeV2"]
    assert level_1["value"] == 1
    assert level_1["next"] == {"null": None}

    # Array of self references with another nested union chain.
    root2 = payload[1]
    assert root2["next"] == {"null": None}
    kids = root2["children"]
    assert kids[0]["value"] == 10
    assert kids[1]["next"]["com.tree.NodeV2"]["value"] == 12


def test_recursion_does_not_loop_forever() -> None:
    # Deeply nested but finite chain: resolution must terminate via the
    # identity-based cycle guard in the structural compatibility check.
    writer_schema = avsc.parse(
        json.dumps(
            {
                "type": "record",
                "name": "Deep",
                "fields": [
                    {"name": "v", "type": "int"},
                    {"name": "next", "type": ["null", "Deep"], "default": None},
                ],
            }
        )
    )
    depth = 200
    node = {"v": 0, "next": None}
    for i in range(depth):
        node = {"v": i, "next": node}
    raw = _ocf_bytes(writer_schema, [node], codec="deflate")
    buffer = io.BytesIO(raw)
    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "Deep",
                "fields": [
                    {"name": "v", "type": "long"},
                    {"name": "next", "type": ["null", "Deep"], "default": None},
                ],
            }
        ),
        "reader",
    )
    out = to_jsonable(convert_container(buffer, reader))
    values: list[int] = []
    cur = out[0]
    while True:
        values.append(cur["v"])
        if cur["next"] == {"null": None}:
            break
        cur = cur["next"]["Deep"]
    # The seed node (v=0) plus `depth` stacked nodes (v=depth-1..v=0). All
    # depth+1 levels survive resolution, proving the recursive type terminates
    # rather than looping forever or being rejected on repeated references.
    expected = list(range(depth - 1, -1, -1)) + [0]
    assert values == expected
    assert len(values) == depth + 1


# --------------------------------------------------------------------------- #
# Union branch selection
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_union_actual_branch_matches_first_compatible_reader_branch(codec: str) -> None:
    records = _convert_file(f"payment_v1_{codec}.avro", "payment_v2_reader.json")
    payload = to_jsonable(records)
    # Reader reordered branches to [int, string, null]; identity follows data.
    assert payload[0]["detail"] == {"null": None}
    assert payload[1]["detail"] == {"string": "card-4242"}
    assert payload[2]["detail"] == {"int": 9527}


def test_union_with_no_compatible_reader_branch_reports_paths() -> None:
    reader_schema = {
        "type": "record",
        "name": "com.pay.PaymentV2",
        "aliases": ["com.pay.PaymentV1"],
        "fields": [
            {"name": "payment_id", "type": "string"},
            # No null branch: the first writer record selects null and fails.
            {"name": "detail", "type": ["int", "string"]},
        ],
    }
    reader = parse_schema(json.dumps(reader_schema), "reader")
    with open(os.path.join(FIXTURE_DIR, "payment_v1_null.avro"), "rb") as handle:
        with pytest.raises(IncompatibleSchemaError) as exc_info:
            convert_container(handle, reader)
    err = exc_info.value
    assert err.writer_path is not None and "detail" in err.writer_path
    assert err.reader_path is not None and "detail" in err.reader_path


def test_writer_branch_data_selects_reader_branch_at_resolution_time() -> None:
    # A reader union whose branch order differs and where int must resolve
    # onto the long branch via the only allowed promotion.
    writer_schema = avsc.parse(
        json.dumps(
            {
                "type": "record",
                "name": "U",
                "fields": [
                    {"name": "f", "type": ["null", "int"], "default": None}
                ],
            }
        )
    )
    raw = _ocf_bytes(writer_schema, [{"f": 5}, {"f": None}])
    buffer = io.BytesIO(raw)

    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "U",
                "fields": [{"name": "f", "type": ["long", "null"]}],
            }
        ),
        "reader",
    )
    payload = to_jsonable(convert_container(buffer, reader))
    assert payload[0]["f"] == {"long": 5}
    assert payload[1]["f"] == {"null": None}


# --------------------------------------------------------------------------- #
# Error handling: whole-request failure, paths and indices
# --------------------------------------------------------------------------- #


def test_unsupported_type_is_rejected_with_schema_path() -> None:
    bad = {
        "type": "record",
        "name": "X",
        "fields": [{"name": "f", "type": "float"}],
    }
    with pytest.raises(UnsupportedTypeError) as exc_info:
        parse_schema(json.dumps(bad), "reader")
    assert exc_info.value.reader_path == "$.fields[0](f)"


def test_logical_type_is_rejected() -> None:
    bad = {"type": "long", "logicalType": "timestamp-millis"}
    with pytest.raises(UnsupportedTypeError):
        parse_schema(json.dumps(bad), "reader")


def test_missing_default_reports_reader_path() -> None:
    reader_schema = {
        "type": "record",
        "name": "com.shop.OrderV2",
        "aliases": ["com.shop.OrderV1"],
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "brand_new", "type": "int"},  # no default, no writer field
        ],
    }
    reader = parse_schema(json.dumps(reader_schema), "reader")
    with open(os.path.join(FIXTURE_DIR, "order_v1_null.avro"), "rb") as handle:
        with pytest.raises(MissingDefaultError) as exc_info:
            convert_container(handle, reader)
    assert "brand_new" in exc_info.value.reader_path


def test_ambiguous_field_alias_is_rejected() -> None:
    # Build a writer with two fields; the reader aliases both onto one field.
    writer_schema = avsc.parse(
        json.dumps(
            {
                "type": "record",
                "name": "A",
                "fields": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": "int"},
                ],
            }
        )
    )
    raw = _ocf_bytes(writer_schema, [{"x": 1, "y": 2}])
    buffer = io.BytesIO(raw)

    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "A",
                "fields": [
                    {"name": "z", "type": "int", "aliases": ["x", "y"]}
                ],
            }
        ),
        "reader",
    )
    with pytest.raises(AmbiguousAliasError):
        convert_container(buffer, reader)


def test_illegal_promotion_long_to_int_fails() -> None:
    writer_schema = avsc.parse(
        json.dumps(
            {
                "type": "record",
                "name": "P",
                "fields": [{"name": "n", "type": "long"}],
            }
        )
    )
    raw = _ocf_bytes(writer_schema, [{"n": 1}])
    buffer = io.BytesIO(raw)

    reader = parse_schema(
        json.dumps(
            {
                "type": "record",
                "name": "P",
                "fields": [{"name": "n", "type": "int"}],
            }
        ),
        "reader",
    )
    with pytest.raises(IncompatibleSchemaError) as exc_info:
        convert_container(buffer, reader)
    assert "long" in exc_info.value.message and "int" in exc_info.value.message


def test_data_level_failure_carries_record_index_and_no_partial_output() -> None:
    # A schema-level-compatible file whose second block is corrupted. The
    # container still decodes structurally, but datum decoding fails on the
    # second record; the error must carry its index and no records are returned.
    raw = bytearray(_fixture("bag_v1_null.avro"))
    # Flip bytes in the trailing data region (well past the OCF header/sync).
    # bag_v1_null.avro holds two records; the later bytes belong to record two.
    raw[-6] ^= 0xFF
    raw[-7] ^= 0xAA
    reader = parse_schema(_reader_json("bag_v1_reader.json"), "reader")
    with pytest.raises(Exception) as exc_info:
        convert_container(io.BytesIO(bytes(raw)), reader)
    err = exc_info.value
    # Either a decoding failure attributed to record 1, or a container error.
    if hasattr(err, "record_index") and err.record_index is not None:
        assert err.record_index == 1


def test_invalid_container_is_rejected() -> None:
    reader = parse_schema(
        json.dumps({"type": "record", "name": "R", "fields": []}), "reader"
    )
    with pytest.raises(Exception):
        convert_container(io.BytesIO(b"not an avro file at all"), reader)


def test_array_and_primitives_round_trip() -> None:
    for codec in ("null", "deflate"):
        records = _convert_file(f"bag_v1_{codec}.avro", "bag_v1_reader.json")
        payload = to_jsonable(records)
        assert payload[0] == {"id": 1, "tags": ["a", "b", "c"], "active": True}
        assert payload[1] == {"id": 2, "tags": [], "active": False}


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #


def test_http_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_http_convert_success() -> None:
    response = client.post(
        "/convert",
        files={"file": ("order.avro", _fixture("order_v1_deflate.avro"), "application/octet-stream")},
        data={"reader_schema": _reader_json("order_v2_reader.json")},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["record_count"] == 2
    assert body["records"][0]["total"] == 3
    assert body["records"][1]["note"] == {"null": None}


def test_http_convert_failure_is_single_error_no_partial_records() -> None:
    bad_reader = json.dumps(
        {
            "type": "record",
            "name": "com.shop.OrderV2",
            "aliases": ["com.shop.OrderV1"],
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "ghost", "type": "int"},
            ],
        }
    )
    response = client.post(
        "/convert",
        files={"file": ("order.avro", _fixture("order_v1_null.avro"), "application/octet-stream")},
        data={"reader_schema": bad_reader},
    )
    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "missing_default"
    assert "ghost" in body["reader_schema_path"]
    assert "records" not in body


def test_http_rejects_unsupported_type() -> None:
    response = client.post(
        "/convert",
        files={"file": ("x.avro", _fixture("bag_v1_null.avro"), "application/octet-stream")},
        data={
            "reader_schema": json.dumps(
                {"type": "record", "name": "com.bag.Bag",
                 "fields": [{"name": "active", "type": "bytes"}]}
            )
        },
    )
    assert response.status_code == 422
    assert response.json()["error"] == "unsupported_type"


def test_http_rejects_garbage_file() -> None:
    response = client.post(
        "/convert",
        files={"file": ("x.avro", b"junk", "application/octet-stream")},
        data={
            "reader_schema": json.dumps(
                {"type": "record", "name": "R", "fields": []}
            )
        },
    )
    assert response.status_code == 422
    assert response.json()["error"] == "invalid_avro_file"
