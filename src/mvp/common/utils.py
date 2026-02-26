"""Common utility functions shared across the project."""

import types
from datetime import date, datetime

import polars as pl
from pydantic import BaseModel

_PYTHON_TO_POLARS: dict[type, pl.DataType] = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bool: pl.Boolean,
    datetime: pl.Datetime,
    date: pl.Date,
}


def polars_schema(model: type[BaseModel]) -> dict[str, pl.DataType]:
    """Derive a full Polars schema from a Pydantic model.

    Maps every field (nullable and non-nullable) to its Polars type.
    For nullable fields (e.g. ``int | None``), extracts the inner type.
    For non-nullable fields (e.g. ``str``), maps directly.
    """
    schema: dict[str, pl.DataType] = {}
    for name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        args = None
        origin = getattr(annotation, "__origin__", None)
        if origin is types.UnionType or origin is type(int | None):
            args = [a for a in annotation.__args__ if a is not type(None)]
        elif hasattr(annotation, "__args__") and type(None) in annotation.__args__:
            args = [a for a in annotation.__args__ if a is not type(None)]

        if args and len(args) == 1:
            inner = args[0]
        else:
            inner = annotation

        pl_type = _PYTHON_TO_POLARS.get(inner)
        if pl_type is None and isinstance(inner, type):
            for base, polars_dt in _PYTHON_TO_POLARS.items():
                if issubclass(inner, base):
                    pl_type = polars_dt
                    break
        if pl_type:
            schema[name] = pl_type
    return schema
