"""Map book odds entries to internal match identifiers."""

import logging

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

SNAPSHOT_SCHEMA = {
    "match_uid": pl.Utf8,
    "book": pl.Utf8,
    "player_id": pl.Utf8,
    "odds": pl.Float64,
    "fetched_at": pl.Datetime("us", "UTC"),
    "event_status": pl.Utf8,
}


def empty() -> pl.DataFrame:
    """Return an empty DataFrame with the standard snapshot schema."""
    return pl.DataFrame(schema=SNAPSHOT_SCHEMA)


def resolve_snapshots(
    staged: pl.DataFrame,
    event_map: pl.DataFrame,
    book: str,
    event_id_col: str,
) -> pl.DataFrame:
    """Resolve staged odds into match_uid + side format.

    Args:
        staged: Raw staged moneyline odds with player_name and event ID column.
        event_map: Event mapping table with match_uid, book, event_id,
                   p1_book_name, p2_book_name.
        book: Book identifier (e.g. "dk", "br", "mgm").
        event_id_col: Name of the event ID column in staged (e.g. "dk_event_id").

    Returns:
        DataFrame with columns: match_uid, book, side, odds, fetched_at,
        event_status. One row per snapshot per side.
    """
    book_map = event_map.filter(pl.col("book") == book)
    if len(book_map) == 0:
        return empty()

    map_cols = ["event_id", "match_uid", "p1_book_name", "p2_book_name"]
    if "p1_id" in book_map.columns:
        map_cols += ["p1_id", "p2_id"]

    joined = staged.join(
        book_map.select(map_cols),
        left_on=event_id_col,
        right_on="event_id",
        how="inner",
    )

    if len(joined) == 0:
        return empty()

    if "p1_id" in joined.columns:
        resolved = joined.with_columns(
            pl.when(pl.col("player_name") == pl.col("p1_book_name"))
            .then(pl.col("p1_id"))
            .when(pl.col("player_name") == pl.col("p2_book_name"))
            .then(pl.col("p2_id"))
            .otherwise(pl.lit(None))
            .alias("player_id"),
            pl.lit(book).alias("book"),
        ).filter(pl.col("player_id").is_not_null())
    else:
        resolved = joined.with_columns(
            pl.when(pl.col("player_name") == pl.col("p1_book_name"))
            .then(pl.lit("p1"))
            .when(pl.col("player_name") == pl.col("p2_book_name"))
            .then(pl.lit("p2"))
            .otherwise(pl.lit(None))
            .alias("player_id"),
            pl.lit(book).alias("book"),
        ).filter(pl.col("player_id").is_not_null())

    cols = ["match_uid", "book", "player_id", "odds", "fetched_at", "event_status"]
    return resolved.select(cols)


def transform(
    event_map: pl.DataFrame,
    stage_input: str,
    book: str,
    event_id_col: str,
    book_label: str,
) -> pl.DataFrame:
    """Load staged odds and resolve through the event map.

    Args:
        event_map: Event mapping table.
        stage_input: Relative path to staged moneyline parquet.
        book: Book identifier for filtering event_map.
        event_id_col: Name of the event ID column in staged data.
        book_label: Human-readable label for log messages (e.g. "DK").
    """
    data_root = get_data_root()
    input_path = data_root / stage_input

    if not input_path.exists():
        logger.warning("%s moneyline parquet not found: %s", book_label, input_path)
        return empty()

    staged = pl.read_parquet(input_path)
    return resolve_snapshots(staged, event_map, book, event_id_col)


def save(df: pl.DataFrame, stage_output: str, book_label: str) -> None:
    """Save resolved snapshots to stage parquet."""
    path = get_data_root() / stage_output
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    logger.info("%s snapshots: %d rows -> %s", book_label, len(df), path)
