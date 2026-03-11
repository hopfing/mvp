"""Shared odds matching utilities used across all book integrations."""

import unicodedata
from dataclasses import dataclass, field


def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching.

    Strips accents (NFKD decomposition), removes hyphens,
    collapses whitespace, lowercases.
    """
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    stripped = stripped.replace("-", " ")
    return " ".join(stripped.lower().split())


@dataclass
class EventMatch:
    """Record of a successful match between a book event and a prediction."""

    match_uid: str
    event_id: str
    p1_book_name: str
    p2_book_name: str


@dataclass
class OddsMatchResult:
    """Result of matching book odds to predictions."""

    odds: dict[str, dict[str, float]] = field(default_factory=dict)
    unmatched_names: set[str] = field(default_factory=set)
    event_matches: list[EventMatch] = field(default_factory=list)
