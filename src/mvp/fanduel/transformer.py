"""Resolve FanDuel moneyline snapshots to match_uid + side."""

import polars as pl

from mvp.common.odds_match_mapper import (
    resolve_snapshots as _resolve,
)
from mvp.common.odds_match_mapper import (
    save as _save,
)
from mvp.common.odds_match_mapper import (
    transform as _transform,
)

STAGE_INPUT = "stage/fanduel/moneyline.parquet"
STAGE_OUTPUT = "stage/fanduel/snapshots.parquet"
EVENT_ID_COL = "fd_event_id"
BOOK = "fd"
BOOK_LABEL = "FD"


def transform(event_map: pl.DataFrame) -> pl.DataFrame:
    """Join FD staged odds through event_map to resolve match_uid and side."""
    return _transform(event_map, STAGE_INPUT, BOOK, EVENT_ID_COL, BOOK_LABEL)


def resolve_snapshots(
    staged: pl.DataFrame,
    event_map: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve staged FD odds into match_uid + side format."""
    return _resolve(staged, event_map, BOOK, EVENT_ID_COL)


def save(df: pl.DataFrame) -> None:
    """Save resolved snapshots to stage parquet."""
    _save(df, STAGE_OUTPUT, BOOK_LABEL)
