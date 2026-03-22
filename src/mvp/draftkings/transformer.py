"""Resolve DraftKings moneyline snapshots to match_uid + side."""

import polars as pl

from mvp.common.odds_match_mapper import (
    resolve_snapshots as _resolve,
    transform as _transform,
    save as _save,
)

STAGE_INPUT = "stage/draftkings/moneyline.parquet"
STAGE_OUTPUT = "stage/draftkings/snapshots.parquet"
EVENT_ID_COL = "dk_event_id"
BOOK = "dk"
BOOK_LABEL = "DK"


def transform(event_map: pl.DataFrame) -> pl.DataFrame:
    """Join DK staged odds through event_map to resolve match_uid and side."""
    return _transform(event_map, STAGE_INPUT, BOOK, EVENT_ID_COL, BOOK_LABEL)


def resolve_snapshots(
    staged: pl.DataFrame,
    event_map: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve staged DK odds into match_uid + side format."""
    return _resolve(staged, event_map, BOOK, EVENT_ID_COL)


def save(df: pl.DataFrame) -> None:
    """Save resolved snapshots to stage parquet."""
    _save(df, STAGE_OUTPUT, BOOK_LABEL)
