"""Tests for pipeline orchestration."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from mvp.common.enums import Circuit

# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_no_args(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args([])
        assert args.year is None
        assert args.tid is None
        assert args.circuit is None
        assert args.refresh is False
        assert args.log_level == "INFO"

    def test_year_only(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--year", "2023"])
        assert args.year == 2023
        assert args.tid is None
        assert args.circuit is None

    def test_tid_with_year(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--tid", "580", "339", "--year", "2023"])
        assert args.tid == ["580", "339"]
        assert args.year == 2023

    def test_tid_values_are_strings(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--tid", "580", "--year", "2023"])
        assert isinstance(args.tid[0], str)

    def test_tid_requires_year(self):
        from mvp.atptour.pipeline import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--tid", "580"])

    def test_circuit_requires_year(self):
        from mvp.atptour.pipeline import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--circuit", "tour"])

    def test_circuit_incompatible_with_tid(self):
        from mvp.atptour.pipeline import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--tid", "580", "--year", "2023", "--circuit", "tour"])

    def test_refresh(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--year", "2023", "--refresh"])
        assert args.refresh is True

    def test_circuit_with_year(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--year", "2023", "--circuit", "tour"])
        assert args.circuit == "tour"

    def test_circuit_chal(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--year", "2023", "--circuit", "chal"])
        assert args.circuit == "chal"

    def test_log_level(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_invalid_log_level(self):
        from mvp.atptour.pipeline import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--log-level", "TRACE"])

    def test_single_tid(self):
        from mvp.atptour.pipeline import parse_args

        args = parse_args(["--tid", "580", "--year", "2023"])
        assert args.tid == ["580"]


# ---------------------------------------------------------------------------
# _resolve_tournaments
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_discovery():
    d = MagicMock()
    d.get_active_tournaments.return_value = [("580", 2026), ("339", 2026)]
    d.get_archive_tournaments.return_value = [
        ("580", 2023, Circuit.tour),
        ("339", 2023, Circuit.tour),
        ("8888", 2023, Circuit.chal),
    ]
    return d


class TestResolveTournaments:
    def test_active_no_args(self, mock_discovery):
        """No args -> get_active_tournaments()"""
        from mvp.atptour.pipeline import _resolve_tournaments

        args = SimpleNamespace(tid=None, year=None, circuit=None)
        result = _resolve_tournaments(args, mock_discovery)

        mock_discovery.get_active_tournaments.assert_called_once()
        assert result == [("580", 2026, False, None), ("339", 2026, False, None)]

    def test_year_archive(self, mock_discovery):
        """Past year -> all archive"""
        from mvp.atptour.pipeline import _resolve_tournaments

        args = SimpleNamespace(tid=None, year=2023, circuit=None)
        with patch("mvp.atptour.pipeline._current_year", return_value=2026):
            result = _resolve_tournaments(args, mock_discovery)

        assert all(is_archive for _, _, is_archive, _ in result)
        assert len(result) == 3

    def test_year_current_checks_active(self, mock_discovery):
        """Current year -> checks active for is_archive"""
        from mvp.atptour.pipeline import _resolve_tournaments

        current_year = datetime.now().year
        mock_discovery.get_archive_tournaments.return_value = [
            ("580", current_year, Circuit.tour),
            ("999", current_year, Circuit.chal),
        ]
        mock_discovery.get_active_tournaments.return_value = [("580", current_year)]

        args = SimpleNamespace(tid=None, year=current_year, circuit=None)
        result = _resolve_tournaments(args, mock_discovery)

        # 580 is active -> not archive; 999 is not active -> archive
        assert result[0] == ("580", current_year, False, Circuit.tour)
        assert result[1] == ("999", current_year, True, Circuit.chal)

    def test_year_with_circuit_filter(self, mock_discovery):
        """--circuit filters archive results"""
        from mvp.atptour.pipeline import _resolve_tournaments

        args = SimpleNamespace(tid=None, year=2023, circuit="tour")
        _resolve_tournaments(args, mock_discovery)

        mock_discovery.get_archive_tournaments.assert_called_once_with(
            2023, circuit=Circuit.tour
        )

    def test_tid_archive(self, mock_discovery):
        """--tid with past year -> all archive"""
        from mvp.atptour.pipeline import _resolve_tournaments

        args = SimpleNamespace(tid=["580", "339"], year=2023, circuit=None)
        result = _resolve_tournaments(args, mock_discovery)

        assert result == [("580", 2023, True, None), ("339", 2023, True, None)]
        mock_discovery.get_archive_tournaments.assert_not_called()

    def test_tid_current_year(self, mock_discovery):
        """--tid with current year -> checks active for is_archive"""
        from mvp.atptour.pipeline import _resolve_tournaments

        current_year = datetime.now().year
        mock_discovery.get_active_tournaments.return_value = [("580", current_year)]

        args = SimpleNamespace(tid=["580", "999"], year=current_year, circuit=None)
        result = _resolve_tournaments(args, mock_discovery)

        # 580 is active -> not archive; 999 not active -> archive
        assert result[0] == ("580", current_year, False, None)
        assert result[1] == ("999", current_year, True, None)


# ---------------------------------------------------------------------------
# _process_tournaments
# ---------------------------------------------------------------------------


class TestProcessTournaments:
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
    ):
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "ATP Test 2023 (580)"
        MockOverviewExt.return_value.run.return_value = mock_tournament

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
    ):
        from mvp.atptour.pipeline import _process_tournaments

        # First tournament fails, second succeeds
        mock_tournament = MagicMock()
        mock_tournament.logging_id = "ATP Test 2023 (339)"
        MockOverviewExt.return_value.run.side_effect = [
            ValueError("API error"),
            mock_tournament,
        ]

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
    ):
        """Archive tournaments: results_refresh=False, stats_refresh=False."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament

        tournaments = [("580", 2023, True, None)]
        _process_tournaments(tournaments, data_root=None, refresh=False)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=False
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=False
        )

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
    ):
        """Active tournaments (not archive, not refresh): results=True, stats=False."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament

        tournaments = [("580", 2026, False, None)]
        _process_tournaments(tournaments, data_root=None, refresh=False)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=False
        )

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
    ):
        """With --refresh: both results and stats refreshed."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament

        tournaments = [("580", 2023, True, None)]
        _process_tournaments(tournaments, data_root=None, refresh=True)

        MockResultsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )
        MockMatchStatsExt.return_value.run.assert_called_once_with(
            mock_tournament, refresh=True
        )

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
    ):
        """data_root is forwarded to all constructors."""
        from mvp.atptour.pipeline import _process_tournaments

        mock_tournament = MagicMock()
        mock_tournament.logging_id = "Test"
        MockOverviewExt.return_value.run.return_value = mock_tournament
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
    ):
        from mvp.atptour.pipeline import _process_tournaments

        failed = _process_tournaments([], data_root=None, refresh=False)
        assert failed == []
        MockOverviewExt.assert_not_called()


# ---------------------------------------------------------------------------
# run_pipeline
# ---------------------------------------------------------------------------


class TestRunPipeline:
    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_rankings_called_first(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = []
        MockProcess.return_value = []
        MockGetPlayers.return_value = {}

        run_pipeline(year=2023)

        MockRankingsExt.assert_called_once_with(start_year=2022, data_root=None)
        MockRankingsExt.return_value.run.assert_called_once()
        MockRankingsTx.return_value.run.assert_called_once_with(start_year=2022)
        MockRankingsTx.return_value.consolidate.assert_called_once()

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_player_data_scoped_to_run(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = [("580", 2023, True, None)]
        MockProcess.return_value = []
        MockGetPlayers.return_value = {
            "FEDERER_R": {("580", 2023), ("339", 2023)},
            "NADAL_R": {("339", 2023)},
        }
        MockBioExt.return_value.run.return_value = []
        MockBioStager.return_value.run.return_value = []
        MockActivityExt.return_value.run.return_value = []
        MockActivityStager.return_value.run.return_value = []

        run_pipeline(year=2023, tournament_ids=["580"])

        # FEDERER has (580, 2023) in run scope; NADAL has only (339, 2023) which isn't
        MockBioExt.return_value.run.assert_called_once()
        player_ids = MockBioExt.return_value.run.call_args[0][0]
        assert player_ids == ["FEDERER_R"]

        MockActivityExt.return_value.run.assert_called_once()
        player_tournaments = MockActivityExt.return_value.run.call_args[0][0]
        assert "FEDERER_R" in player_tournaments
        assert player_tournaments["FEDERER_R"] == {("580", 2023)}
        assert "NADAL_R" not in player_tournaments

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_skips_player_data_when_no_players(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = []
        MockProcess.return_value = []
        MockGetPlayers.return_value = {}

        run_pipeline(year=2023)

        MockBioExt.return_value.run.assert_not_called()
        MockBioStager.return_value.run.assert_not_called()
        MockBioTx.return_value.run.assert_not_called()
        MockActivityExt.return_value.run.assert_not_called()
        MockActivityStager.return_value.run.assert_not_called()
        MockActivityTx.return_value.run.assert_not_called()

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_raises_on_tournament_failures(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = [("580", 2023, True, None)]
        MockProcess.return_value = [("580", 2023, "boom")]
        MockGetPlayers.return_value = {}

        with pytest.raises(RuntimeError, match="1 failed tournament"):
            run_pipeline(year=2023)

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_raises_on_player_failures(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = [("580", 2023, True, None)]
        MockProcess.return_value = []
        MockGetPlayers.return_value = {"FEDERER_R": {("580", 2023)}}
        MockBioExt.return_value.run.return_value = [("FEDERER_R", "404")]
        MockBioStager.return_value.run.return_value = []
        MockActivityExt.return_value.run.return_value = []
        MockActivityStager.return_value.run.return_value = []

        with pytest.raises(RuntimeError, match="failed player operation"):
            run_pipeline(year=2023, tournament_ids=["580"])

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_default_start_year_no_year_arg(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        """No year arg -> start_year = current_year - 1."""
        from mvp.atptour.pipeline import run_pipeline

        MockResolve.return_value = []
        MockProcess.return_value = []
        MockGetPlayers.return_value = {}

        run_pipeline()

        expected_start = datetime.now().year - 1
        MockRankingsExt.assert_called_once_with(
            start_year=expected_start, data_root=None
        )

    @patch("mvp.atptour.pipeline.PlayerActivityTransformer")
    @patch("mvp.atptour.pipeline.PlayerActivityStager")
    @patch("mvp.atptour.pipeline.PlayerActivityExtractor")
    @patch("mvp.atptour.pipeline.PlayerBioTransformer")
    @patch("mvp.atptour.pipeline.PlayerBioStager")
    @patch("mvp.atptour.pipeline.PlayerBioExtractor")
    @patch("mvp.atptour.pipeline.get_active_players")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.pipeline._resolve_tournaments")
    @patch("mvp.atptour.pipeline.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.RankingsTransformer")
    @patch("mvp.atptour.pipeline.RankingsExtractor")
    def test_data_root_propagated(
        self,
        MockRankingsExt,
        MockRankingsTx,
        MockDiscovery,
        MockResolve,
        MockProcess,
        MockGetPlayers,
        MockBioExt,
        MockBioStager,
        MockBioTx,
        MockActivityExt,
        MockActivityStager,
        MockActivityTx,
    ):
        from mvp.atptour.pipeline import run_pipeline

        data_root = Path("/tmp/test_data")
        MockResolve.return_value = [("580", 2023, True, None)]
        MockProcess.return_value = []
        MockGetPlayers.return_value = {"FEDERER_R": {("580", 2023)}}
        MockBioExt.return_value.run.return_value = []
        MockBioStager.return_value.run.return_value = []
        MockActivityExt.return_value.run.return_value = []
        MockActivityStager.return_value.run.return_value = []

        run_pipeline(data_root=data_root, year=2023, tournament_ids=["580"])

        MockRankingsExt.assert_called_once_with(start_year=2022, data_root=data_root)
        MockDiscovery.assert_called_once_with(data_root=data_root)
        MockProcess.assert_called_once()
        assert MockProcess.call_args[1]["data_root"] == data_root
        MockBioExt.assert_called_once_with(data_root=data_root)
        MockBioStager.assert_called_once_with(data_root=data_root)
        MockBioTx.assert_called_once_with(data_root=data_root)
        MockActivityExt.assert_called_once_with(data_root=data_root)
        MockActivityStager.assert_called_once_with(data_root=data_root)
        MockActivityTx.assert_called_once_with(data_root=data_root)


# ---------------------------------------------------------------------------
# main (CLI entry point)
# ---------------------------------------------------------------------------


class TestMain:
    @patch("mvp.atptour.pipeline.run_pipeline")
    @patch("mvp.atptour.pipeline.parse_args")
    def test_main_wires_args_to_run_pipeline(self, mock_parse, mock_run):
        from mvp.atptour.pipeline import main

        mock_parse.return_value = SimpleNamespace(
            log_level="INFO",
            year=2023,
            tid=["580"],
            circuit=None,
            refresh=True,
        )

        main()

        mock_run.assert_called_once_with(
            year=2023,
            tournament_ids=["580"],
            circuit=None,
            refresh=True,
        )
