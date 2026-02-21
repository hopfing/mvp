"""Schema hash computation for drift detection."""

import hashlib

from pydantic import BaseModel


def compute_schema_hash(model: type[BaseModel]) -> str:
    """Compute a deterministic hash from a model's field names and types.

    Returns a 16-character hex string. Any field addition, removal, or
    type change produces a different hash. Used alongside SCHEMA_VERSION
    for automatic drift detection per ADR-001.
    """
    fields = sorted(model.model_fields.items())
    parts = [f"{name}:{info.annotation}" for name, info in fields]
    content = "|".join(parts)
    return hashlib.sha256(content.encode()).hexdigest()[:16]
