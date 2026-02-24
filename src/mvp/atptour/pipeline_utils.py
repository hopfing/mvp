"""Pipeline utility functions for player activity lookups."""

from __future__ import annotations

from pathlib import Path

import polars as pl

_SKIP_IDS = {"0", "AAA1", "AAA2", "AAA3", "AAA4", "AAA5", "AAA6", "AAA7", "AAA8"}

DAVIS_CUP_TIDS = {"8096", "8097", "8099"}


def get_active_players(
    tournaments_stage_dir: Path,
) -> dict[str, set[tuple[str, int]]]:
    """Map player IDs to tournament appearances from staged results.

    Returns dict mapping player_id to set of (tournament_id, year) tuples.
    Skips placeholder IDs.
    """
    player_tournaments: dict[str, set[tuple[str, int]]] = {}

    id_columns = ["p1_id", "p2_id", "p1_partner_id", "p2_partner_id"]
    for path in sorted(tournaments_stage_dir.rglob("results.parquet")):
        available = pl.read_parquet_schema(path)
        cols_to_read = ["tournament_id", "year"] + [
            c for c in id_columns if c in available
        ]
        df = pl.read_parquet(path, columns=cols_to_read)
        for row in df.iter_rows(named=True):
            tid_year = (row["tournament_id"], row["year"])
            for col in id_columns:
                pid = row.get(col)
                if pid and pid not in _SKIP_IDS:
                    player_tournaments.setdefault(pid, set()).add(tid_year)

    return player_tournaments


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
