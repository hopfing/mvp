"""Common utility functions shared across the project."""

import types
from datetime import date

import polars as pl
from pydantic import BaseModel

_PYTHON_TO_POLARS: dict[type, pl.DataType] = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bool: pl.Boolean,
    date: pl.Date,
}


def polars_schema_overrides(model: type[BaseModel]) -> dict[str, pl.DataType]:
    """Derive Polars schema_overrides for all nullable fields on a Pydantic model."""
    overrides = {}
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
            pl_type = _PYTHON_TO_POLARS.get(inner)
            if pl_type is None and isinstance(inner, type):
                for base, polars_dt in _PYTHON_TO_POLARS.items():
                    if issubclass(inner, base):
                        pl_type = polars_dt
                        break
            if pl_type:
                overrides[name] = pl_type
    return overrides
