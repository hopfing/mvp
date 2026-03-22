"""Resolve BetRivers moneyline snapshots to match_uid + side."""

import polars as pl

from mvp.common.odds_match_mapper import (
    resolve_snapshots as _resolve,
    transform as _transform,
    save as _save,
)

STAGE_INPUT = "stage/betrivers/moneyline.parquet"
STAGE_OUTPUT = "stage/betrivers/snapshots.parquet"
EVENT_ID_COL = "br_event_id"
BOOK = "br"
BOOK_LABEL = "BR"


def transform(event_map: pl.DataFrame) -> pl.DataFrame:
    """Join BR staged odds through event_map to resolve match_uid and side."""
    return _transform(event_map, STAGE_INPUT, BOOK, EVENT_ID_COL, BOOK_LABEL)


def resolve_snapshots(
    staged: pl.DataFrame,
    event_map: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve staged BR odds into match_uid + side format."""
    return _resolve(staged, event_map, BOOK, EVENT_ID_COL)


def save(df: pl.DataFrame) -> None:
    """Save resolved snapshots to stage parquet."""
    _save(df, STAGE_OUTPUT, BOOK_LABEL)
