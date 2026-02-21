"""Shared schema validation helpers and utilities.

Cross-field validation functions used by model validators across schemas.
Each takes explicit arguments (no Pydantic dependency) so they're testable
in isolation.
"""

import hashlib
from typing import Any

from pydantic import BaseModel

from mvp.common.enums import DrawType
from mvp.common.mappings import is_placeholder_id


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


def validate_winner_in_players(winner_id: str, p1_id: str, p2_id: str) -> None:
    """Raise ValueError if winner_id is not one of the player IDs."""
    if winner_id not in (p1_id, p2_id):
        raise ValueError(
            f"winner_id '{winner_id}' must be either p1_id '{p1_id}' or p2_id '{p2_id}'"
        )


def validate_doubles_partners(draw_type: DrawType, partner_values: list[Any]) -> None:
    """Raise ValueError if partner fields are inconsistent with draw type."""
    if draw_type == DrawType.doubles:
        if any(v is None for v in partner_values):
            raise ValueError("All partner fields must be non-null for doubles")
    else:
        if any(v is not None for v in partner_values):
            raise ValueError("All partner fields must be null for singles")


def validate_match_uid_placeholders(
    match_uid: str | None, player_ids: list[str]
) -> None:
    """Raise ValueError if match_uid is inconsistent with placeholder IDs."""
    has_placeholder = any(
        is_placeholder_id(pid) for pid in player_ids if pid is not None
    )
    if has_placeholder and match_uid is not None:
        raise ValueError("match_uid must be null when any player ID is a placeholder")
    if not has_placeholder and match_uid is None:
        raise ValueError(
            "match_uid must be non-null when no player IDs are placeholders"
        )
