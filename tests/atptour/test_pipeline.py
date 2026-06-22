"""Tests for pipeline orchestration."""


import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from mvp.common.enums import Circuit

# ---------------------------------------------------------------------------
# _process_tournaments
# ---------------------------------------------------------------------------


class TestProcessTournaments:
    @patch("mvp.atptour.pipeline._feed_stage_is_stale")
    @patch("mvp.atptour.pipeline.StatsPlusTransformer")
    @patch("mvp.atptour.pipeline.RallyAnalysisTransformer")
    @patch("mvp.atptour.pipeline.StrokeAnalysisTransformer")
    @patch("mvp.atptour.pipeline.MatchBeatsTransformer")
    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_happy_path(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
        MockMatchBeatsTx,
        MockStrokeTx,
        MockRallyTx,
        MockStatsPlusTx,
        MockStale,
    ):
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "ATP Test 2023 (580)"
        MockOverviewExt.return_value.run.return_value = mock_tournament
        MockMatchStatsExt.return_value.run.return_value = 3
        MockMatchCentreExt.return_value.run.return_value = 2
        # Match-centre feeds have unstaged/newer raw -> transforms run.
        MockStale.return_value = True

        tournaments = [("580", 2023, True, Circuit.tour)]
        failed = _process_tournaments(tournaments, data_root=None, refresh=False)

        assert failed == []
        MockOverviewExt.return_value.run.assert_called_once_with(
            tournament_id="580",
            year=2023,
            is_archive=True,
            refresh=False,
            circuit=Circuit.tour,
        )
        MockOverviewTx.assert_called_once_with(mock_tournament, data_root=None)
        MockOverviewTx.return_value.run.assert_called_once()
        MockScheduleExt.return_value.run.assert_called_once_with(mock_tournament)
        MockScheduleTx.return_value.run.assert_called_once()
        MockResultsExt.return_value.run.assert_called_once()
        MockResultsTx.return_value.run.assert_called_once()
        MockMatchStatsExt.return_value.run.assert_called_once()
        MockMatchStatsTx.return_value.run.assert_called_once()
        MockMatchBeatsTx.return_value.run.assert_called_once()
        MockStrokeTx.return_value.run.assert_called_once()
        MockRallyTx.return_value.run.assert_called_once()
        MockStatsPlusTx.return_value.run.assert_called_once()

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_failure_continues(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        from mvp.atptour.pipeline import _process_tournaments

        # "580" fails, "339" succeeds — use argument-based side_effect
        # so thread execution order doesn't matter
        mock_tournament = MagicMock()
        mock_tournament.logging_id = "ATP Test 2023 (339)"

        def overview_side_effect(**kwargs):
            if kwargs.get("tournament_id") == "580":
                raise ValueError("API error")
            return mock_tournament

        MockOverviewExt.return_value.run.side_effect = overview_side_effect
        MockMatchStatsExt.return_value.run.return_value = 1
        MockMatchCentreExt.return_value.run.return_value = 1

        tournaments = [
            ("580", 2023, True, None),
            ("339", 2023, True, None),
        ]
        failed = _process_tournaments(tournaments, data_root=None, refresh=False)

        assert len(failed) == 1
        assert failed[0][0] == "580"
        assert failed[0][1] == 2023
        assert "API error" in failed[0][2]
        # Second tournament still processed
        MockOverviewTx.return_value.run.assert_called_once()

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_archive_no_refresh(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        """Archive tournaments: results always refresh, stats only when refresh=True."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament
        MockMatchStatsExt.return_value.run.return_value = 1
        MockMatchCentreExt.return_value.run.return_value = 0

        tournaments = [("580", 2023, True, None)]
        _process_tournaments(tournaments, data_root=None, refresh=False)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True  # Results always refresh
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=False
        )

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_active_refresh_strategy(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        """Active tournaments (not archive, not refresh): results=True, stats=False."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament
        MockMatchStatsExt.return_value.run.return_value = 0
        MockMatchCentreExt.return_value.run.return_value = 0

        tournaments = [("580", 2026, False, None)]
        _process_tournaments(tournaments, data_root=None, refresh=False)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=False
        )

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_forced_refresh(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        """With --refresh: both results and stats refreshed."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament
        MockMatchStatsExt.return_value.run.return_value = 1
        MockMatchCentreExt.return_value.run.return_value = 1

        tournaments = [("580", 2023, True, None)]
        _process_tournaments(tournaments, data_root=None, refresh=True)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_data_root_passed_through(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        """data_root is forwarded to all constructors."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament
        MockMatchStatsExt.return_value.run.return_value = 1
        MockMatchCentreExt.return_value.run.return_value = 1
        data_root = Path("/tmp/test_data")

        tournaments = [("580", 2023, True, None)]
        _process_tournaments(tournaments, data_root=data_root, refresh=False)

        MockOverviewExt.assert_called_once_with(data_root=data_root)
        MockOverviewTx.assert_called_once_with(mock_tournament, data_root=data_root)
        MockScheduleExt.assert_called_once_with(data_root=data_root)
        MockScheduleTx.assert_called_once_with(mock_tournament, data_root=data_root)
        MockResultsExt.assert_called_once_with(data_root=data_root)
        MockResultsTx.assert_called_once_with(mock_tournament, data_root=data_root)
        MockMatchStatsExt.assert_called_once_with(data_root=data_root)
        MockMatchStatsTx.assert_called_once_with(mock_tournament, data_root=data_root)


class TestFeedStageIsStale:
    """The match-centre staging gate: (re)stage when raw is unstaged or newer
    than the stage, decoupled from the extract event."""

    def _transformer(self, tmp_path):
        from mvp.atptour.tournament import Tournament
        from mvp.atptour.transformers.stats_plus import StatsPlusTransformer

        tournament = Tournament(
            tournament_id="339",
            year=2023,
            circuit=Circuit.tour,
            location="Indian Wells, USA",
        )
        return StatsPlusTransformer(tournament, data_root=tmp_path)

    def _paths(self, tx):
        path = tx.tournament.path
        return (
            tx.build_path("raw", path, "stats_plus"),
            tx.build_path("stage", path, "stats_plus.parquet"),
        )

    def test_no_raw_dir_not_stale(self, tmp_path):
        from mvp.atptour.pipeline import _feed_stage_is_stale

        tx = self._transformer(tmp_path)
        assert _feed_stage_is_stale(tx, "stats_plus") is False

    def test_empty_raw_dir_not_stale(self, tmp_path):
        from mvp.atptour.pipeline import _feed_stage_is_stale

        tx = self._transformer(tmp_path)
        raw_dir, _ = self._paths(tx)
        raw_dir.mkdir(parents=True, exist_ok=True)
        assert _feed_stage_is_stale(tx, "stats_plus") is False

    def test_missing_stage_is_stale(self, tmp_path):
        from mvp.atptour.pipeline import _feed_stage_is_stale

        tx = self._transformer(tmp_path)
        raw_dir, _ = self._paths(tx)
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "MS001.json").write_text("{}")
        assert _feed_stage_is_stale(tx, "stats_plus") is True

    def test_stage_newer_not_stale(self, tmp_path):
        from mvp.atptour.pipeline import _feed_stage_is_stale

        tx = self._transformer(tmp_path)
        raw_dir, out = self._paths(tx)
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw = raw_dir / "MS001.json"
        raw.write_text("{}")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("x")
        os.utime(raw, (1000, 1000))
        os.utime(out, (2000, 2000))
        assert _feed_stage_is_stale(tx, "stats_plus") is False

    def test_raw_newer_is_stale(self, tmp_path):
        from mvp.atptour.pipeline import _feed_stage_is_stale

        tx = self._transformer(tmp_path)
        raw_dir, out = self._paths(tx)
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw = raw_dir / "MS001.json"
        raw.write_text("{}")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("x")
        os.utime(out, (1000, 1000))
        os.utime(raw, (2000, 2000))
        assert _feed_stage_is_stale(tx, "stats_plus") is True

    def test_schema_drift_is_stale(self, tmp_path):
        """A stage with a current mtime but a stale metadata schema hash must
        re-stage when a hash is supplied (stats_plus); with no hash (sibling
        feeds) it must not."""
        import polars as pl

        from mvp.atptour.pipeline import _feed_stage_is_stale
        from mvp.atptour.schemas.stats_plus import SCHEMA_HASH

        tx = self._transformer(tmp_path)
        raw_dir, out = self._paths(tx)
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw = raw_dir / "MS001.json"
        raw.write_text("{}")
        out.parent.mkdir(parents=True, exist_ok=True)
        # Stage carries a STALE pydantic_schema_hash but is newer than raw.
        pl.DataFrame({"x": [1]}).write_parquet(
            out, metadata={"pydantic_schema_hash": "stale_hash"}
        )
        os.utime(raw, (1000, 1000))
        os.utime(out, (2000, 2000))
        assert _feed_stage_is_stale(tx, "stats_plus", SCHEMA_HASH) is True
        assert _feed_stage_is_stale(tx, "stats_plus", None) is False

    @patch("mvp.atptour.pipeline.MatchCentreExtractor")
    @patch("mvp.atptour.pipeline.MatchStatsTransformer")
    @patch("mvp.atptour.pipeline.MatchStatsExtractor")
    @patch("mvp.atptour.pipeline.ResultsTransformer")
    @patch("mvp.atptour.pipeline.ResultsExtractor")
    @patch("mvp.atptour.pipeline.ScheduleTransformer")
    @patch("mvp.atptour.pipeline.ScheduleExtractor")
    @patch("mvp.atptour.pipeline.OverviewTransformer")
    @patch("mvp.atptour.pipeline.OverviewExtractor")
    def test_empty_tournaments(
        self,
        MockOverviewExt,
        MockOverviewTx,
        MockScheduleExt,
        MockScheduleTx,
        MockResultsExt,
        MockResultsTx,
        MockMatchStatsExt,
        MockMatchStatsTx,
        MockMatchCentreExt,
    ):
        from mvp.atptour.pipeline import _process_tournaments

        failed = _process_tournaments([], data_root=None, refresh=False)
        assert failed == []
        MockOverviewExt.assert_not_called()


# ---------------------------------------------------------------------------
# run_rankings
# ---------------------------------------------------------------------------


class TestRunRankings:
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_runs_extract_transform_consolidate(self, MockRankingsExt, MockRankingsTx):
        from mvp.atptour.pipeline import run_rankings

        MockRankingsExt.return_value.run.return_value = 3

        run_rankings(start_year=2022, data_root=None)

        MockRankingsExt.assert_called_once_with(start_year=2022, data_root=None)
        MockRankingsExt.return_value.run.assert_called_once()
        MockRankingsTx.assert_called_once_with(data_root=None)
        MockRankingsTx.return_value.run.assert_called_once_with(start_year=2022)
        MockRankingsTx.return_value.consolidate.assert_called_once()

    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_skips_transform_when_no_new_pages(self, MockRankingsExt, MockRankingsTx):
        from mvp.atptour.pipeline import run_rankings

        MockRankingsExt.return_value.run.return_value = 0

        run_rankings(start_year=2022, data_root=None)

        MockRankingsExt.return_value.run.assert_called_once()
        MockRankingsTx.return_value.run.assert_not_called()
        MockRankingsTx.return_value.consolidate.assert_not_called()

    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_passes_data_root(self, MockRankingsExt, MockRankingsTx):
        from mvp.atptour.pipeline import run_rankings

        MockRankingsExt.return_value.run.return_value = 1
        data_root = Path("/tmp/test")
        run_rankings(start_year=2022, data_root=data_root)

        MockRankingsExt.assert_called_once_with(start_year=2022, data_root=data_root)
        MockRankingsTx.assert_called_once_with(data_root=data_root)


# ---------------------------------------------------------------------------
# run_player_data
# ---------------------------------------------------------------------------


class TestRunPlayerData:
    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    def test_scopes_to_run_tids(
        self, MockGetPlayers, MockBioExt, MockBioStager, MockBioTx,
        MockActivityExt, MockActivityStager, MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_player_data

        MockGetPlayers.return_value = {
            "FEDERER_R": {("580", 2023), ("339", 2023)},
            "NADAL_R": {("339", 2023)},
        }
        MockBioExt.return_value.run.return_value = ([], 1)
        MockBioStager.return_value.run.return_value = []
        MockActivityExt.return_value.run.return_value = []
        MockActivityStager.return_value.run.return_value = []

        run_tids = {("580", 2023)}
        result = run_player_data(run_tids=run_tids, data_root=None)

        player_ids = MockBioExt.return_value.run.call_args[0][0]
        assert player_ids == ["FEDERER_R"]
        assert result.failed_bio_fetch == []

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    def test_skips_when_no_players(
        self, MockGetPlayers, MockBioExt, MockBioStager, MockBioTx,
        MockActivityExt, MockActivityStager, MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_player_data

        MockGetPlayers.return_value = {}

        result = run_player_data(run_tids={("580", 2023)}, data_root=None)

        MockBioExt.return_value.run.assert_not_called()
        MockActivityExt.return_value.run.assert_not_called()
        assert result.has_failures is False

    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    def test_skips_bio_stager_when_no_new_bios(
        self, MockGetPlayers, MockBioExt, MockBioStager, MockBioTx,
    ):
        from mvp.atptour.pipeline import run_player_data

        MockGetPlayers.return_value = {
            "FEDERER_R": {("580", 2023)},
        }
        MockBioExt.return_value.run.return_value = ([], 0)

        run_player_data(run_tids={("580", 2023)}, data_root=None)

        MockBioStager.return_value.run.assert_not_called()
        MockBioTx.return_value.run.assert_not_called()

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.get_players_with_results")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    def test_activity_skipped_without_refresh_players(
        self, MockGetPlayers, MockBioExt, MockBioStager, MockBioTx,
        MockGetResults, MockActivityExt, MockActivityStager, MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_player_data

        MockGetPlayers.return_value = {
            "FEDERER_R": {("580", 2023)},
        }
        MockBioExt.return_value.run.return_value = ([], 0)

        result = run_player_data(run_tids={("580", 2023)}, data_root=None)

        MockActivityExt.return_value.run.assert_not_called()
        assert result.failed_activity_fetch == []

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.get_players_with_results")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    def test_activity_runs_with_refresh_players(
        self, MockGetPlayers, MockBioExt, MockBioStager, MockBioTx,
        MockGetResults, MockActivityExt, MockActivityStager, MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_player_data

        MockGetPlayers.return_value = {
            "FEDERER_R": {("580", 2023)},
        }
        MockBioExt.return_value.run.return_value = ([], 0)
        MockBioStager.return_value.run.return_value = []
        MockGetResults.return_value = set()
        MockActivityExt.return_value.run.return_value = []
        MockActivityStager.return_value.run.return_value = []

        result = run_player_data(
            run_tids={("580", 2023)}, data_root=None, refresh_players=True
        )

        MockActivityExt.return_value.run.assert_called_once()
        assert result.failed_activity_fetch == []
