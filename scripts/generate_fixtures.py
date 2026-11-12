#!/usr/bin/env python3
"""Generate real Avro Object Container Files used by the test-suite.

Run with an interpreter that has `avro` installed, e.g. (from repo root):

    python -I scripts/generate_fixtures.py /abs/path/to/tests/fixtures

Everything written here is produced through avro.datafile.DataFileWriter, so
the fixtures are genuine OCF containers (header + sync markers + blocks),
exercising both the `null` and `deflate` codecs.
"""
from __future__ import annotations

import json
import os
import sys

import avro.datafile as avdf
import avro.io as avio
import avro.schema as avsc


def write_ocf(path: str, schema: avsc.Schema, records: list[dict], codec: str) -> None:
    with open(path, "wb") as handle:
        writer = avdf.DataFileWriter(handle, avio.DatumWriter(), schema, codec=codec)
        for record in records:
            writer.append(record)
        writer.flush()
        writer.close()


def load(schema: dict) -> avsc.Schema:
    return avsc.parse(json.dumps(schema))


# --------------------------------------------------------------------------- #
# Scenario 1: field rename via alias, int->long promotion, new field defaults,
# deleted field, field reordering, explicit null preserved, nested record.
# --------------------------------------------------------------------------- #
ORDER_V1 = {
    "type": "record",
    "name": "com.shop.OrderV1",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "count", "type": "int"},
        {"name": "note", "type": ["null", "string"], "default": None},
        {
            "name": "customer",
            "type": {
                "type": "record",
                "name": "com.shop.CustomerV1",
                "fields": [
                    {"name": "cid", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            },
        },
        {"name": "legacy_flag", "type": "boolean"},
    ],
}

ORDER_V1_RECORDS = [
    # count:int = 3 -> reader total:long via alias; note is a real string.
    {
        "order_id": "o-1001",
        "count": 3,
        "note": "rush",
        "customer": {"cid": 7, "name": "Ada"},
        "legacy_flag": True,
    },
    # Explicit null in a union field: must stay null, never become the default.
    {
        "order_id": "o-1002",
        "count": 10,
        "note": None,
        "customer": {"cid": 8, "name": "Lin"},
        "legacy_flag": False,
    },
]

ORDER_V2 = {
    "type": "record",
    "name": "com.shop.OrderV2",
    "aliases": ["com.shop.OrderV1"],
    "fields": [
        # Reordered: total now comes first and reads writer field `count`.
        {"name": "total", "type": "long", "aliases": ["count"]},
        {
            "name": "customer",
            "type": {
                "type": "record",
                "name": "com.shop.CustomerV2",
                "aliases": ["com.shop.CustomerV1"],
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "customer_id", "type": "long", "aliases": ["cid"]},
                ],
            },
        },
        {"name": "order_id", "type": "string"},
        # New field absent in writer -> must take its default ("pending").
        {"name": "status", "type": "string", "default": "pending"},
        # Reader note is non-union string; default only applies when the writer
        # field is missing. Here the writer field exists, so explicit null must
        # surface as an incompatibility-safe case instead: keep the same union
        # shape and use a default that must NOT override explicit null.
        {
            "name": "note",
            "type": ["null", "string"],
            "default": None,
        },
    ],
}

# --------------------------------------------------------------------------- #
# Scenario 2: recursive record chain (linked list), repeated self reference,
# deleted field and reordering on the recursive node itself.
# --------------------------------------------------------------------------- #
NODE_V1 = {
    "type": "record",
    "name": "com.tree.NodeV1",
    "fields": [
        {"name": "value", "type": "int"},
        {"name": "next", "type": ["null", "com.tree.NodeV1"], "default": None},
        {"name": "children", "type": {"type": "array", "items": "com.tree.NodeV1"}},
        {"name": "obsolete", "type": "string"},
    ],
}

NODE_V1_RECORDS = [
    {
        # 3 -> 2 -> 1 -> null chain through the self-referential union.
        "value": 3,
        "next": {
            "value": 2,
            "next": {
                "value": 1,
                "next": None,
                "children": [],
                "obsolete": "leaf",
            },
            "children": [],
            "obsolete": "mid",
        },
        "children": [],
        "obsolete": "root",
    },
    # A second root exercising the array-of-self reference and branching.
    {
        "value": 9,
        "next": None,
        "children": [
            {"value": 10, "next": None, "children": [], "obsolete": "c1"},
            {
                "value": 11,
                "next": {"value": 12, "next": None, "children": [], "obsolete": "c2n"},
                "children": [],
                "obsolete": "c2",
            },
        ],
        "obsolete": "root2",
    },
]

NODE_V2 = {
    "type": "record",
    "name": "com.tree.NodeV2",
    "aliases": ["com.tree.NodeV1"],
    "fields": [
        # Reordered: children before next; obsolete field deleted; new default.
        {"name": "children", "type": {"type": "array", "items": "com.tree.NodeV2"}},
        {"name": "next", "type": ["null", "com.tree.NodeV2"], "default": None},
        {"name": "value", "type": "long"},
        {"name": "depth", "type": "int", "default": 0},
    ],
}

# --------------------------------------------------------------------------- #
# Scenario 3: union branch selection and reordering between writer and reader.
# --------------------------------------------------------------------------- #
PAYMENT_V1 = {
    "type": "record",
    "name": "com.pay.PaymentV1",
    "fields": [
        {"name": "payment_id", "type": "string"},
        # Writer branch order: null, string, int.
        {"name": "detail", "type": ["null", "string", "int"], "default": None},
    ],
}

PAYMENT_V1_RECORDS = [
    {"payment_id": "p1", "detail": None},
    {"payment_id": "p2", "detail": "card-4242"},
    {"payment_id": "p3", "detail": 9527},
]

PAYMENT_V2 = {
    "type": "record",
    "name": "com.pay.PaymentV2",
    "aliases": ["com.pay.PaymentV1"],
    "fields": [
        {"name": "payment_id", "type": "string"},
        # Reader branch order changed: int first, then string, then null.
        {"name": "detail", "type": ["int", "string", "null"], "default": 0},
    ],
}

# --------------------------------------------------------------------------- #
# Scenario 4: plain array + primitive round trip under null and deflate codecs.
# --------------------------------------------------------------------------- #
BAG_V1 = {
    "type": "record",
    "name": "com.bag.Bag",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "active", "type": "boolean"},
    ],
}

BAG_V1_RECORDS = [
    {"id": 1, "tags": ["a", "b", "c"], "active": True},
    {"id": 2, "tags": [], "active": False},
]


def main(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    order_writer = load(ORDER_V1)
    write_ocf(os.path.join(out_dir, "order_v1_null.avro"), order_writer, ORDER_V1_RECORDS, "null")
    write_ocf(os.path.join(out_dir, "order_v1_deflate.avro"), order_writer, ORDER_V1_RECORDS, "deflate")
    with open(os.path.join(out_dir, "order_v2_reader.json"), "w", encoding="utf-8") as fh:
        json.dump(ORDER_V2, fh, ensure_ascii=False, indent=2)

    write_ocf(os.path.join(out_dir, "node_v1_deflate.avro"), load(NODE_V1), NODE_V1_RECORDS, "deflate")
    with open(os.path.join(out_dir, "node_v2_reader.json"), "w", encoding="utf-8") as fh:
        json.dump(NODE_V2, fh, ensure_ascii=False, indent=2)

    payment_writer = load(PAYMENT_V1)
    write_ocf(os.path.join(out_dir, "payment_v1_null.avro"), payment_writer, PAYMENT_V1_RECORDS, "null")
    write_ocf(os.path.join(out_dir, "payment_v1_deflate.avro"), payment_writer, PAYMENT_V1_RECORDS, "deflate")
    with open(os.path.join(out_dir, "payment_v2_reader.json"), "w", encoding="utf-8") as fh:
        json.dump(PAYMENT_V2, fh, ensure_ascii=False, indent=2)

    bag_writer = load(BAG_V1)
    write_ocf(os.path.join(out_dir, "bag_v1_null.avro"), bag_writer, BAG_V1_RECORDS, "null")
    write_ocf(os.path.join(out_dir, "bag_v1_deflate.avro"), bag_writer, BAG_V1_RECORDS, "deflate")
    with open(os.path.join(out_dir, "bag_v1_reader.json"), "w", encoding="utf-8") as fh:
        json.dump(BAG_V1, fh, ensure_ascii=False, indent=2)

    print("Fixtures written to", out_dir)
    for name in sorted(os.listdir(out_dir)):
        size = os.path.getsize(os.path.join(out_dir, name))
        print(f"  {name:30s} {size:6d} bytes")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python -I generate_fixtures.py <output-directory>", file=sys.stderr)
        raise SystemExit(2)
    main(os.path.abspath(sys.argv[1]))
