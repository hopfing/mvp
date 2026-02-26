"""PlayerActivity stager and transformer (consolidator)."""

import datetime as dt
import logging
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.player_activity import PlayerActivityRecord
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema_overrides

logger = logging.getLogger(__name__)


def _derive_tiebreak_scores(set_player, set_opponent, set_tie):
    if set_tie is None:
        return None, None
    loser_tb = set_tie
    winner_tb = max(7, loser_tb + 2)
    if (
        set_player is not None
        and set_opponent is not None
        and set_player > set_opponent
    ):
        return winner_tb, loser_tb
    return loser_tb, winner_tb


def _parse_activity_json(
    player_id: str,
    data: dict | None,
    source_file: str,
    parsed_at: dt.datetime,
) -> list[PlayerActivityRecord]:
    if data is None:
        return []
    records = []
    for year_block in data.get("Activity") or []:
        event_year = year_block["EventYear"]
        for t in year_block["Tournaments"]:
            event_id = t["EventId"]
            event_type = t["EventType"]
            surface = t.get("Surface")
            in_outdoor = t.get("InOutdoor")
            player_rank = t.get("PlayerRank")
            tournament_start_date = t["EventDate"]
            tournament_end_date = t["PlayEndDate"]
            points = t["Points"]
            prize_usd = t["PrizeUsd"]
            for m in t["Matches"]:
                set_scores = {}
                for n in range(1, 6):
                    sp = m.get(f"Set{n}Player")
                    so = m.get(f"Set{n}Opponent")
                    p_tb, o_tb = _derive_tiebreak_scores(sp, so, m.get(f"Set{n}Tie"))
                    set_scores[f"player_set{n}_games"] = sp
                    set_scores[f"opp_set{n}_games"] = so
                    set_scores[f"player_set{n}_tiebreak"] = p_tb
                    set_scores[f"opp_set{n}_tiebreak"] = o_tb
                record = PlayerActivityRecord(
                    player_id=player_id,
                    year=int(event_year),
                    tournament_id=str(event_id),
                    event_type=event_type,
                    surface=surface,
                    indoor=in_outdoor,
                    tournament_start_date=tournament_start_date,
                    tournament_end_date=tournament_end_date,
                    points=points,
                    prize_usd=prize_usd,
                    match_id=str(m["MatchId"]),
                    round=m["Round"]["ShortName"],
                    win_loss=m["WinLoss"],
                    reason=m.get("Reason"),
                    player_rank=player_rank,
                    opp_id=m["OpponentId"],
                    opp_first_name=m.get("OpponentFirstName"),
                    opp_last_name=m.get("OpponentLastName"),
                    opp_natl_id=m.get("OpponentNatlId"),
                    opp_rank=m.get("OpponentRank"),
                    **set_scores,
                    has_stats=m["HasStats"],
                    match_stats_url=m.get("MatchStatsUrl"),
                    is_bye=m["IsBye"],
                    source_file=source_file,
                    parsed_at=parsed_at,
                )
                records.append(record)
    return records


class PlayerActivityStager(BaseJob):
    """Parse raw player activity JSON into per-player staged parquets."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> list[tuple[str, str]]:
        raw_dir = self.build_path("raw", "activity")
        raw_files = self.list_files(raw_dir, "*.json")
        if not raw_files:
            return []

        stage_dir = self.build_path("stage", "activity")
        existing = {p.stem: p for p in self.list_files(stage_dir, "*.parquet")}

        to_process = []
        for raw_path in raw_files:
            pid = raw_path.stem
            staged_path = existing.get(pid)
            if (
                staged_path is None
                or raw_path.stat().st_mtime > staged_path.stat().st_mtime
            ):
                to_process.append(raw_path)

        to_process.sort(key=lambda p: p.stem)
        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)

        failed: list[tuple[str, str]] = []
        for raw_path in to_process:
            pid = raw_path.stem
            try:
                data = self.read_json(raw_path)
                source_file = str(self._display_path(raw_path))
                records = _parse_activity_json(pid, data, source_file, parsed_at)
                if not records:
                    continue
                df = pl.DataFrame(
                    [r.model_dump() for r in records],
                    schema_overrides=polars_schema_overrides(PlayerActivityRecord),
                )
                target = self.build_path("stage", "activity", f"{pid}.parquet")
                self.save_parquet(df, target)
            except Exception as e:
                logger.warning("Failed to stage activity for %s: %s", pid, e)
                failed.append((pid, str(e)))

        logger.info(
            "Player activity stager: %d raw files, %d to process, %d failed",
            len(raw_files),
            len(to_process),
            len(failed),
        )
        return failed


class PlayerActivityTransformer(BaseJob):
    """Consolidate per-player parquets into a single activity.parquet."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> Path | None:
        stage_dir = self.build_path("stage", "activity")
        parquet_files = self.list_files(stage_dir, "*.parquet")
        if not parquet_files:
            logger.info("No player activity parquets to consolidate")
            return None

        dfs = [pl.read_parquet(p) for p in parquet_files]
        combined = pl.concat(dfs, how="diagonal_relaxed")

        key_cols = ["player_id", "tournament_id", "year", "match_id"]
        before = len(combined)
        combined = combined.unique(subset=key_cols)
        if len(combined) < before:
            logger.warning(
                "Dropped %d duplicate activity rows", before - len(combined)
            )

        target = self.build_path("stage", "activity.parquet")
        result = self.save_parquet(combined, target)

        logger.info(
            "Player activity consolidate: merged %d files, %d total rows",
            len(parquet_files),
            len(combined),
        )
        return result

    @staticmethod
    def _assert_unique(df: pl.DataFrame, key_cols: list[str]) -> None:
        """Assert primary key uniqueness."""
        dupes = df.group_by(key_cols).len().filter(pl.col("len") > 1)
        if len(dupes) > 0:
            samples = dupes.head(5)[key_cols].to_dicts()
            raise ValueError(
                f"Duplicate primary keys in player_activity: {samples}"
            )
