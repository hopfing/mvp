"""Pipeline utility functions for player activity lookups."""


from pathlib import Path

import polars as pl

from mvp.atptour.mappings import PLACEHOLDER_IDS, is_placeholder_id

DAVIS_CUP_TIDS = {"8096", "8097", "8099"}


def get_scheduled_pairs(
    tournaments_stage_dir: Path,
    run_tids: set[tuple[str, int]] | None = None,
) -> pl.DataFrame:
    """(player_id, match_uid, scheduled_datetime) for every scheduled singles match.

    One row per player per match, so both sides of a match appear. Read from the
    deduplicated `schedule.parquet` rather than per-snapshot files: `_dedup` in the
    schedule transformer deliberately drops superseded draw entries, and fetching on
    an entry ATP has already retracted is wasted work against a host that has flagged
    this IP.

    Rows with a null `match_uid` are dropped. That is not a data-quality filter -- a
    null uid means at least one side is still a placeholder, and a match with an
    unresolved player cannot be predicted, so there is nothing yet to keep current.
    """
    frames = []
    want = ["tournament_id", "year", "draw_type", "match_uid",
            "p1_id", "p2_id", "scheduled_datetime"]
    for path in sorted(tournaments_stage_dir.rglob("schedule.parquet")):
        available = pl.read_parquet_schema(path)
        if "match_uid" not in available:
            continue
        df = pl.read_parquet(path, columns=[c for c in want if c in available])
        df = df.filter((pl.col("draw_type") == "singles")
                       & pl.col("match_uid").is_not_null())
        if run_tids is not None:
            keep = pl.struct("tournament_id", "year").map_elements(
                lambda s: (s["tournament_id"], s["year"]) in run_tids,
                return_dtype=pl.Boolean)
            df = df.filter(keep)
        if df.is_empty():
            continue
        frames.append(
            pl.concat([
                df.select("match_uid", "scheduled_datetime",
                          pl.col(c).alias("player_id"))
                for c in ("p1_id", "p2_id") if c in df.columns
            ])
        )
    if not frames:
        return pl.DataFrame(schema={"player_id": pl.String, "match_uid": pl.String,
                                    "scheduled_datetime": pl.Datetime("us")})
    out = pl.concat(frames).drop_nulls("player_id")
    # Native `is_in` over the frozenset rather than a per-row UDF -- this runs every
    # tick and polars warns loudly enough to bury the activity logs otherwise.
    out = out.filter(~pl.col("player_id").is_in(list(PLACEHOLDER_IDS)))
    return out.unique(["player_id", "match_uid"])


def get_active_players(
    tournaments_stage_dir: Path,
    *,
    source: str = "schedule",
) -> dict[str, set[tuple[str, int]]]:
    """Map player IDs to tournament appearances from staged data.

    Args:
        source: "schedule" for live (who's in the draw), "results" for backfill.

    Returns dict mapping player_id to set of (tournament_id, year) tuples.
    Skips placeholder IDs.
    """
    filename = f"{source}.parquet"
    player_tournaments: dict[str, set[tuple[str, int]]] = {}

    id_columns = ["p1_id", "p2_id"]
    for path in sorted(tournaments_stage_dir.rglob(filename)):
        available = pl.read_parquet_schema(path)
        cols_to_read = ["tournament_id", "year", "draw_type"] + [
            c for c in id_columns if c in available
        ]
        df = pl.read_parquet(path, columns=cols_to_read)
        df = df.filter(pl.col("draw_type") == "singles")
        for row in df.iter_rows(named=True):
            tid_year = (row["tournament_id"], row["year"])
            for col in id_columns:
                pid = row.get(col)
                if pid and not is_placeholder_id(pid):
                    player_tournaments.setdefault(pid, set()).add(tid_year)

    return player_tournaments


def get_players_with_results(
    tournaments_stage_dir: Path,
    run_tids: set[tuple[str, int]],
) -> set[str]:
    """Return player IDs that already have results in any of the run tournaments."""
    players: set[str] = set()
    id_columns = ["p1_id", "p2_id"]
    for path in sorted(tournaments_stage_dir.rglob("results.parquet")):
        available = pl.read_parquet_schema(path)
        cols_to_read = ["tournament_id", "year", "draw_type"] + [
            c for c in id_columns if c in available
        ]
        df = pl.read_parquet(path, columns=cols_to_read)
        df = df.filter(pl.col("draw_type") == "singles")
        for row in df.iter_rows(named=True):
            tid_year = (row["tournament_id"], row["year"])
            if tid_year not in run_tids:
                continue
            for col in id_columns:
                pid = row.get(col)
                if pid and not is_placeholder_id(pid):
                    players.add(pid)
    return players


def activity_covers_tournament(
    activity_json: dict | None, year: int, tournament_id: str
) -> bool:
    """Check whether a player's activity JSON includes a specific tournament.

    Handles Davis Cup specially: any tournament with EventType "DC" matches
    any Davis Cup tournament ID.
    """
    if activity_json is None:
        return False
    is_davis_cup = tournament_id in DAVIS_CUP_TIDS
    for year_block in activity_json.get("Activity", []) or []:
        if int(year_block["EventYear"]) != year:
            continue
        for t in year_block["Tournaments"]:
            if is_davis_cup:
                if t.get("EventType") == "DC":
                    return True
            elif str(t["EventId"]) == tournament_id:
                return True
    return False
