"""ATP-specific schema validation helpers.

Cross-field validation functions used by model validators across ATP schemas.
Each takes explicit arguments so they're testable in isolation.
"""

from typing import Any

from mvp.common.enums import DrawType

from .mappings import is_placeholder_id


def parse_indoor(v) -> bool | None:
    """Parse indoor/outdoor indicator to boolean."""
    if v is None or v == "":
        return None
    if v == "I":
        return True
    if v == "O":
        return False
    if isinstance(v, bool):
        return v
    raise ValueError(f"Unknown InOutdoor value '{v}'. Expected 'I', 'O', or ''.")


def empty_to_none(v):
    """Convert empty strings to None, pass through everything else."""
    if v == "":
        return None
    return v


def strip_or_none(v: str | None) -> str | None:
    """Strip whitespace from string; return None for empty/whitespace-only strings."""
    if isinstance(v, str):
        return v.strip() or None
    return v


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
