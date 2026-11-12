"""Domain errors for the Avro schema-resolution conversion service.

Every failure carries enough location information to pinpoint the offending
schema element (and, when data has already been read) the record index.
The HTTP layer turns these into structured 422 responses; the request as a
whole never returns partially converted records.
"""
from __future__ import annotations

from typing import Any, Optional


class ConversionError(Exception):
    """Base class for all conversion failures."""

    code = "conversion_error"

    def __init__(
        self,
        message: str,
        *,
        writer_path: Optional[str] = None,
        reader_path: Optional[str] = None,
        record_index: Optional[int] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.writer_path = writer_path
        self.reader_path = reader_path
        self.record_index = record_index
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"error": self.code, "message": self.message}
        if self.writer_path is not None:
            payload["writer_schema_path"] = self.writer_path
        if self.reader_path is not None:
            payload["reader_schema_path"] = self.reader_path
        if self.record_index is not None:
            payload["record_index"] = self.record_index
        if self.details:
            payload["details"] = self.details
        return payload


class UnsupportedTypeError(ConversionError):
    """A schema uses a type outside the supported whitelist."""

    code = "unsupported_type"


class SchemaParseFailure(ConversionError):
    """The supplied Avro schema JSON cannot be parsed."""

    code = "invalid_schema_json"


class IncompatibleSchemaError(ConversionError):
    """Writer and reader schemas cannot be resolved per the Avro spec."""

    code = "incompatible_schemas"


class AmbiguousAliasError(ConversionError):
    """A reader field alias matches more than one writer field."""

    code = "ambiguous_alias"


class MissingDefaultError(ConversionError):
    """A reader field is absent in the writer and declares no default."""

    code = "missing_default"


class InvalidDefaultError(ConversionError):
    """A declared default cannot be interpreted under the field schema."""

    code = "invalid_default"


class UnsupportedCodecError(ConversionError):
    """The container file uses a compression codec other than null/deflate."""

    code = "unsupported_codec"


class ContainerError(ConversionError):
    """The uploaded bytes are not a valid Avro Object Container File."""

    code = "invalid_avro_file"
