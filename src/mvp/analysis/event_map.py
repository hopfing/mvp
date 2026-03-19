"""Event mapping: book event IDs <-> match_uids."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.common.odds_matching import EventMatch

logger = logging.getLogger(__name__)

EVENT_MAP_SCHEMA = {
    "match_uid": pl.Utf8,
    "book": pl.Utf8,
    "event_id": pl.Utf8,
    "p1_book_name": pl.Utf8,
    "p2_book_name": pl.Utf8,
    "matched_at": pl.Datetime("us", "UTC"),
    "source": pl.Utf8,
}

DEFAULT_PATH = get_data_root() / "odds" / "event_map.parquet"
DEFAULT_OVERRIDE_PATH = get_data_root() / "odds" / "event_overrides.yaml"


def load_event_map(path: Path = DEFAULT_PATH) -> pl.DataFrame:
    """Load existing event map, or return empty DataFrame with correct schema."""
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=EVENT_MAP_SCHEMA)


def save_event_mappings(
    matches: list[EventMatch],
    book: str,
    path: Path = DEFAULT_PATH,
) -> None:
    """Append new event mappings, skipping duplicates on (match_uid, book)."""
    if not matches:
        return

    now = datetime.now(timezone.utc)
    new_df = pl.DataFrame([
        {
            "match_uid": m.match_uid,
            "book": book,
            "event_id": m.event_id,
            "p1_book_name": m.p1_book_name,
            "p2_book_name": m.p2_book_name,
            "matched_at": now,
            "source": "auto",
        }
        for m in matches
    ])

    existing = load_event_map(path)

    if len(existing) > 0:
        existing_keys = existing.select("match_uid", "book")
        new_df = new_df.join(existing_keys, on=["match_uid", "book"], how="anti")
        if len(new_df) == 0:
            return
        combined = pl.concat([existing, new_df], how="diagonal_relaxed")
    else:
        combined = new_df

    path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(path)
    logger.info("Event map: %d total mappings (%d new)", len(combined), len(new_df))


def load_event_map_with_overrides(
    path: Path = DEFAULT_PATH,
    override_path: Path = DEFAULT_OVERRIDE_PATH,
) -> pl.DataFrame:
    """Load event map with manual overrides merged in."""
    df = load_event_map(path)

    if override_path.exists():
        overrides = yaml.safe_load(override_path.read_text()) or []
        if overrides:
            now = datetime.now(timezone.utc)
            override_df = pl.DataFrame([
                {**entry, "matched_at": now, "source": "manual"}
                for entry in overrides
            ])
            # Manual overrides take precedence
            if len(df) > 0:
                df = df.join(
                    override_df.select("match_uid", "book"),
                    on=["match_uid", "book"],
                    how="anti",
                )
            df = pl.concat([df, override_df], how="diagonal_relaxed")

    return df
