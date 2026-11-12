"""Avro Object Container File ingestion and per-record conversion.

The library's :class:`DataFileReader` is used only for the container envelope
(magic/header/blocks, sync markers and null/deflate decompression). Its datum
reader is replaced here so that:

* decoding always happens against the *writer's* schema (the reader schema is
  never passed into a library decoder), and
* union branch identity is retained via :class:`Tagged`.

Schema resolution itself lives in :mod:`app.resolver`.
"""
from __future__ import annotations

from typing import Any, BinaryIO

import avro.datafile as avdf
import avro.io as avio
import avro.schema as avsc

from .errors import (
    ContainerError,
    ConversionError,
    UnsupportedCodecError,
)
from .resolver import check_compatible, read_writer_value, resolve_value
from .schema_validate import validate_supported

#: Codecs the service accepts inside the container metadata.
SUPPORTED_CODECS = frozenset({"null", "deflate"})


class _TaggedDatumReader(avio.DatumReader):
    """DatumReader that decodes with the writer schema and keeps union tags."""

    def read(self, decoder: avio.BinaryDecoder) -> Any:  # type: ignore[override]
        if self.writers_schema is None:
            raise ContainerError("Container header did not provide a writer schema.")
        return read_writer_value(self.writers_schema, decoder)


def parse_schema(schema_text: str, side: str) -> avsc.Schema:
    """Parse reader-supplied Avro schema JSON with clear error reporting."""
    try:
        schema = avsc.parse(schema_text)
    except Exception as exc:  # avro raises several parse exception types
        raise ConversionError(
            f"Could not parse {side} schema: {exc}",
        ) from exc
    validate_supported(schema, side)
    return schema


def _codec_of(reader: avdf.DataFileReader) -> str:
    codec = reader.codec
    if codec not in SUPPORTED_CODECS:
        raise UnsupportedCodecError(
            f"Container uses codec '{codec}', only 'null' and 'deflate' are supported.",
            details={"codec": codec},
        )
    return codec


def convert_container(
    file_obj: BinaryIO, reader_schema: avsc.Schema
) -> list[Any]:
    """Read every record from an OCF and resolve it under ``reader_schema``.

    Either every record is converted and returned, or a single
    :class:`ConversionError` is raised; no partial result is ever returned.
    """
    datum_reader = _TaggedDatumReader()
    try:
        container = avdf.DataFileReader(file_obj, datum_reader)
    except Exception as exc:
        raise ContainerError(
            f"Not a valid Avro Object Container File: {exc}"
        ) from exc

    try:
        writer_schema = container.datum_reader.writers_schema
        if writer_schema is None:
            raise ContainerError("Container header did not provide a writer schema.")
        validate_supported(writer_schema, "writer")
        _codec_of(container)

        # Structural pre-flight: schema-level incompatibilities fail before any
        # record data is interpreted, and bad defaults fail up front.
        check_compatible(writer_schema, reader_schema)

        converted: list[Any] = []
        record_index = 0
        while True:
            try:
                raw_value = next(container)
            except StopIteration:
                break
            except ConversionError:
                raise
            except Exception as exc:
                raise ContainerError(
                    f"Failed to decode record {record_index}: {exc}",
                ) from exc

            try:
                converted.append(
                    resolve_value(
                        writer_schema,
                        reader_schema,
                        raw_value,
                        writer_path="$",
                        reader_path="$",
                        record_index=record_index,
                    )
                )
            except ConversionError as exc:
                # Ensure the record index is always populated for data errors.
                if exc.record_index is None:
                    exc.record_index = record_index
                raise
            record_index += 1

        return converted
    finally:
        container.close()
