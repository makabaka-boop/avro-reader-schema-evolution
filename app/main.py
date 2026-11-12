"""FastAPI application exposing the Avro schema-resolution service."""
from __future__ import annotations

import io
from typing import Any

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse

from .avro_json import to_jsonable
from .container import convert_container, parse_schema
from .errors import ContainerError, ConversionError

app = FastAPI(
    title="Avro Schema Resolution Service",
    version="1.0.0",
    description=(
        "Accepts a real Avro Object Container File plus a reader schema and "
        "returns every record interpreted under the reader schema, performing "
        "Avro 1.11.x writer->reader resolution (aliases, defaults, field "
        "reordering/removal, int->long promotion, union branch selection)."
    ),
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/convert")
async def convert(
    file: UploadFile = File(..., description="Avro Object Container File (.avro)"),
    reader_schema: str = Form(..., description="Reader Avro schema as JSON text"),
) -> JSONResponse:
    """Convert all records; any failure rejects the whole request."""
    try:
        reader = parse_schema(reader_schema, "reader")
        file_bytes = await file.read()
        if not file_bytes:
            raise ContainerError("Uploaded Avro file is empty.")
        # Feed the library a binary stream over the in-memory upload bytes.
        records = convert_container(io.BytesIO(file_bytes), reader)
    except ConversionError as exc:
        return JSONResponse(status_code=422, content=exc.to_dict())

    payload: dict[str, Any] = {
        "record_count": len(records),
        "records": [to_jsonable(record) for record in records],
    }
    return JSONResponse(status_code=200, content=payload)
