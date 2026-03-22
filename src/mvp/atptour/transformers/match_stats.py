"""Transform raw match stats JSON into staged parquet via MatchStatsRecord schema."""

import datetime as dt
import logging
from datetime import date
from pathlib import Path

import polars as pl

from mvp.atptour.mappings import (
    map_player_id,
    parse_duration,
    parse_seed_entry,
)
from mvp.atptour.schemas.match_stats import MatchStatsRecord
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.enums import DrawType
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)


class MatchStatsTransformer(BaseJob):
    """Transform raw match stats JSON into staged parquet.

    Processes all match_stats/*.json files for a tournament, producing
    a single match_stats.parquet file.
    """

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> list[Path]:
        """Process all match stats JSON files. Returns parquet paths (0 or 1)."""
        stats_dir = self.build_path("raw", self.tournament.path, "match_stats")
        json_files = self.list_files(stats_dir, "*.json")

        if not json_files:
            logger.info(
                "No match stats files for %s", self.tournament.logging_id
            )
            return []

        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        records: list[MatchStatsRecord] = []

        for json_file in json_files:
            data = self.read_json(json_file)
            source_file = str(self._display_path(json_file))

            if data is None:
                logger.debug("Null JSON in %s, skipping", source_file)
                continue

            player_team = data["Match"]["PlayerTeam"]
            set_scores = player_team["SetScores"]

            if not set_scores:
                logger.debug("Empty SetScores in %s, skipping", source_file)
                continue

            if set_scores[0]["Stats"] is None:
                logger.debug("Null Stats in %s, skipping", source_file)
                continue

            record = self._build_record(data, source_file, parsed_at)
            records.append(record)

        if not records:
            logger.info(
                "No valid match stats records for %s", self.tournament.logging_id
            )
            return []

        rows = [r.model_dump() for r in records]
        overrides = polars_schema(MatchStatsRecord)
        df = pl.DataFrame(rows, schema_overrides=overrides)

        self.assert_unique(df, ["match_uid"], "match_stats")

        out_path = self.build_path(
            "stage", self.tournament.path, "match_stats.parquet"
        )
        result = self.save_parquet(df, out_path)
        if result is None:
            return []
        return [result]

    def _build_record(
        self,
        data: dict,
        source_file: str,
        parsed_at: dt.datetime,
    ) -> MatchStatsRecord:
        """Map a raw JSON structure to a validated MatchStatsRecord."""
        tournament_data = data["Tournament"]
        match_data = data["Match"]
        player_team = match_data["PlayerTeam"]
        opponent_team = match_data["OpponentTeam"]
        player_team1 = match_data["PlayerTeam1"]
        player_team2 = match_data["PlayerTeam2"]

        is_doubles = match_data["IsDoubles"]
        draw_type = DrawType.doubles if is_doubles else DrawType.singles

        round_name = match_data["Round"]["LongName"]

        p1_id = map_player_id(player_team["Player"]["PlayerId"])
        p2_id = map_player_id(opponent_team["Player"]["PlayerId"])

        p1_partner_id = None
        p2_partner_id = None
        if player_team.get("Partner") is not None:
            p1_partner_id = map_player_id(player_team["Partner"]["PlayerId"])
        if opponent_team.get("Partner") is not None:
            p2_partner_id = map_player_id(opponent_team["Partner"]["PlayerId"])

        winner_raw = match_data.get("Winner")
        winner_id = map_player_id(winner_raw) if winner_raw else None

        duration_raw = match_data.get("MatchTimeTotal")
        duration_seconds = None
        if duration_raw:
            try:
                duration_seconds = parse_duration(duration_raw)
            except ValueError:
                logger.debug(
                    "Unparseable duration '%s' in %s", duration_raw, source_file
                )

        # Compute sets_played from SetScores length (minus the summary set at index 0)
        sets_played = max(len(player_team["SetScores"]) - 1, 0)

        # Extract stats
        p1_stats = self._extract_side_stats(player_team)
        p2_stats = self._extract_side_stats(opponent_team)

        p1_prefixed = {f"p1_{k}": v for k, v in p1_stats.items()}
        p2_prefixed = {f"p2_{k}": v for k, v in p2_stats.items()}

        p1_seed_val, p1_entry_val = parse_seed_entry(player_team1.get("SeedPlayerTeam"))
        p2_seed_val, p2_entry_val = parse_seed_entry(player_team2.get("SeedPlayerTeam"))

        return MatchStatsRecord(
            tournament_id=self.tournament.tournament_id,
            year=self.tournament.year,
            circuit=self.tournament.circuit,
            draw_type=draw_type,
            round=round_name,
            round_id=match_data["Round"].get("RoundId"),
            match_id=match_data["MatchId"],
            surface=tournament_data.get("Court"),
            tournament_start_date=self._parse_date(tournament_data.get("StartDate")),
            tournament_end_date=self._parse_date(tournament_data.get("EndDate")),
            tournament_city=tournament_data.get("TournamentCity"),
            prize_money=tournament_data.get("PrizeMoney"),
            currency=tournament_data.get("CurrencySymbol"),
            draw_size_singles=tournament_data.get("Singles"),
            draw_size_doubles=tournament_data.get("Doubles"),
            winner_id=winner_id,
            duration_seconds=duration_seconds,
            reason=match_data.get("Reason"),
            number_of_sets=match_data["NumberOfSets"],
            sets_played=sets_played,
            is_qualifier=match_data.get("IsQualifier"),
            scoring_system=match_data.get("ScoringSystem"),
            court_name=match_data.get("CourtName"),
            umpire_first_name=match_data.get("UmpireFirstName"),
            umpire_last_name=match_data.get("UmpireLastName"),
            p1_id=p1_id,
            p2_id=p2_id,
            p1_partner_id=p1_partner_id,
            p2_partner_id=p2_partner_id,
            p1_seed=p1_seed_val,
            p1_entry=p1_entry_val,
            p2_seed=p2_seed_val,
            p2_entry=p2_entry_val,
            source_file=source_file,
            parsed_at=parsed_at,
            **p1_prefixed,
            **p2_prefixed,
        )

    @staticmethod
    def _extract_side_stats(team_data: dict) -> dict:
        """Extract 26 stat fields from one side's SetScores[0].Stats.

        Returns unprefixed keys (e.g., svc_aces, ret_bp_converted).
        Caller adds p1_/p2_ prefix.
        """
        stats = team_data["SetScores"][0]["Stats"]
        svc = stats["ServiceStats"]
        ret = stats["ReturnStats"]
        pts = stats["PointStats"]

        return {
            "svc_aces": svc["Aces"]["Number"],
            "svc_double_faults": svc["DoubleFaults"]["Number"],
            "svc_first_serve_in": svc["FirstServe"]["Dividend"],
            "svc_first_serve_att": svc["FirstServe"]["Divisor"],
            "svc_first_serve_pts_won": svc["FirstServePointsWon"]["Dividend"],
            "svc_first_serve_pts_played": svc["FirstServePointsWon"]["Divisor"],
            "svc_second_serve_pts_won": svc["SecondServePointsWon"]["Dividend"],
            "svc_second_serve_pts_played": svc["SecondServePointsWon"]["Divisor"],
            "svc_bp_saved": svc["BreakPointsSaved"]["Dividend"],
            "svc_bp_faced": svc["BreakPointsSaved"]["Divisor"],
            "svc_games_played": svc["ServiceGamesPlayed"]["Number"],
            "svc_serve_rating": svc["ServeRating"]["Number"],
            "ret_first_serve_pts_won": ret["FirstServeReturnPointsWon"]["Dividend"],
            "ret_first_serve_pts_played": ret["FirstServeReturnPointsWon"]["Divisor"],
            "ret_second_serve_pts_won": ret["SecondServeReturnPointsWon"]["Dividend"],
            "ret_second_serve_pts_played": ret["SecondServeReturnPointsWon"]["Divisor"],
            "ret_bp_converted": ret["BreakPointsConverted"]["Dividend"],
            "ret_bp_opportunities": ret["BreakPointsConverted"]["Divisor"],
            "ret_games_played": ret["ReturnGamesPlayed"]["Number"],
            "ret_return_rating": ret["ReturnRating"]["Number"],
            "pts_service_pts_won": pts["TotalServicePointsWon"]["Dividend"],
            "pts_service_pts_played": pts["TotalServicePointsWon"]["Divisor"],
            "pts_return_pts_won": pts["TotalReturnPointsWon"]["Dividend"],
            "pts_return_pts_played": pts["TotalReturnPointsWon"]["Divisor"],
            "pts_total_pts_won": pts["TotalPointsWon"]["Dividend"],
            "pts_total_pts_played": pts["TotalPointsWon"]["Divisor"],
        }

    @staticmethod
    def _parse_date(iso_str: str | None) -> date | None:
        """Parse ISO 8601 datetime string to date, or None if null/empty."""
        if not iso_str:
            return None
        try:
            return dt.datetime.fromisoformat(iso_str).date()
        except (ValueError, TypeError):
            return None

