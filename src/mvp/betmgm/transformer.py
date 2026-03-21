"""Resolve BetMGM moneyline snapshots to match_uid + side."""

import logging

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

STAGE_INPUT = "stage/betmgm/moneyline.parquet"
STAGE_OUTPUT = "stage/betmgm/snapshots.parquet"
EVENT_ID_COL = "mgm_event_id"
BOOK = "mgm"


def transform(event_map: pl.DataFrame) -> pl.DataFrame:
    """Join MGM staged odds through event_map to resolve match_uid and side.

    Args:
        event_map: Event mapping table with match_uid, book, event_id,
                   p1_book_name, p2_book_name.

    Returns:
        DataFrame with columns: match_uid, book, side, odds, fetched_at,
        event_status. One row per snapshot per side.
    """
    data_root = get_data_root()
    input_path = data_root / STAGE_INPUT

    if not input_path.exists():
        logger.warning("MGM moneyline parquet not found: %s", input_path)
        return _empty()

    staged = pl.read_parquet(input_path)
    return resolve_snapshots(staged, event_map)


def resolve_snapshots(
    staged: pl.DataFrame,
    event_map: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve staged MGM odds into match_uid + side format."""
    book_map = event_map.filter(pl.col("book") == BOOK)
    if len(book_map) == 0:
        return _empty()

    joined = staged.join(
        book_map.select("event_id", "match_uid", "p1_book_name", "p2_book_name"),
        left_on=EVENT_ID_COL,
        right_on="event_id",
        how="inner",
    )

    if len(joined) == 0:
        return _empty()

    resolved = joined.with_columns(
        pl.when(pl.col("player_name") == pl.col("p1_book_name"))
        .then(pl.lit("p1"))
        .when(pl.col("player_name") == pl.col("p2_book_name"))
        .then(pl.lit("p2"))
        .otherwise(pl.lit(None))
        .alias("side"),
        pl.lit(BOOK).alias("book"),
    ).filter(pl.col("side").is_not_null())

    cols = ["match_uid", "book", "side", "odds", "fetched_at", "event_status"]
    return resolved.select(cols)


def save(df: pl.DataFrame) -> None:
    """Save resolved snapshots to stage parquet."""
    path = get_data_root() / STAGE_OUTPUT
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    logger.info("MGM snapshots: %d rows -> %s", len(df), path)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "book": pl.Utf8,
        "side": pl.Utf8,
        "odds": pl.Float64,
        "fetched_at": pl.Datetime("us", "UTC"),
        "event_status": pl.Utf8,
    })
