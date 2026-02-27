"""Tests for CLI subcommand argument parsing and command dispatch."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestParseArgsLive:
    def test_live_no_args(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["live"])
        assert args.command == "live"
        assert args.tid is None
        assert args.refresh is False
        assert args.log_level == "INFO"

    def test_live_with_tid(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["live", "--tid", "580"])
        assert args.command == "live"
        assert args.tid == "580"

    def test_live_with_refresh(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["live", "--refresh"])
        assert args.refresh is True

    def test_live_tid_is_single_string(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["live", "--tid", "580"])
        assert isinstance(args.tid, str)


class TestParseArgsBackfill:
    def test_backfill_requires_year(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args(["backfill"])

    def test_backfill_with_year(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["backfill", "--year", "2023"])
        assert args.command == "backfill"
        assert args.year == 2023
        assert args.tid is None
        assert args.circuit is None

    def test_backfill_with_tids(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["backfill", "--year", "2023", "--tid", "580", "339"])
        assert args.tid == ["580", "339"]

    def test_backfill_with_circuit(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["backfill", "--year", "2023", "--circuit", "tour"])
        assert args.circuit == "tour"

    def test_backfill_circuit_and_tid_incompatible(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args(
                ["backfill", "--year", "2023", "--tid", "580", "--circuit", "tour"]
            )

    def test_backfill_tid_values_are_strings(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["backfill", "--year", "2023", "--tid", "580"])
        assert isinstance(args.tid[0], str)


class TestParseArgsModel:
    def test_model_no_args(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["model"])
        assert args.command == "model"
        assert args.config is None

    def test_model_with_config(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["model", "--config", "experiments/gbt-v1.yaml"])
        assert args.config == "experiments/gbt-v1.yaml"


class TestParseArgsShared:
    def test_log_level_before_subcommand(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["--log-level", "DEBUG", "live"])
        assert args.log_level == "DEBUG"
        assert args.command == "live"

    def test_invalid_log_level(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--log-level", "TRACE", "live"])

    def test_no_subcommand_exits(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args([])


class TestCmdLive:
    @patch("mvp.atptour.cli.MatchesAggregator")
    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_active_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_live_full_chain(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
        MockMatchesAgg,
    ):
        from mvp.atptour.cli import cmd_live

        MockResolve.return_value = [("580", 2026, False, None)]
        MockProcess.return_value = []
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid=None, refresh=False)
        cmd_live(args)

        MockRankings.assert_called_once()
        MockResolve.assert_called_once()
        MockProcess.assert_called_once()
        MockPlayerData.assert_called_once()
        MockMatchesAgg.return_value.run.assert_called_once()

    @patch("mvp.atptour.cli.MatchesAggregator")
    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_active_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_live_with_tid_filters(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
        MockMatchesAgg,
    ):
        from mvp.atptour.cli import cmd_live

        MockResolve.return_value = [("580", 2026, False, None)]
        MockProcess.return_value = []
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid="580", refresh=False)
        cmd_live(args)

        MockResolve.assert_called_once_with(tid="580")

    @patch("mvp.atptour.cli.MatchesAggregator")
    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_active_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_live_raises_on_failures(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
        MockMatchesAgg,
    ):
        from mvp.atptour.cli import cmd_live

        MockResolve.return_value = [("580", 2026, False, None)]
        MockProcess.return_value = [("580", 2026, "boom")]
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid=None, refresh=False)
        with pytest.raises(RuntimeError, match="failed tournament"):
            cmd_live(args)


class TestCmdBackfill:
    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_backfill_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_backfill_no_aggregation(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
    ):
        from mvp.atptour.cli import cmd_backfill
        from mvp.common.enums import Circuit

        MockResolve.return_value = [("580", 2023, True, Circuit.tour)]
        MockProcess.return_value = []
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(year=2023, tid=None, circuit=None)
        cmd_backfill(args)

        MockRankings.assert_called_once()
        MockProcess.assert_called_once()
        MockPlayerData.assert_called_once()

    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_backfill_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_backfill_passes_year_and_circuit(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
    ):
        from mvp.atptour.cli import cmd_backfill

        MockResolve.return_value = []
        MockProcess.return_value = []
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(year=2023, tid=None, circuit="chal")
        cmd_backfill(args)

        MockResolve.assert_called_once_with(year=2023, tid=None, circuit="chal")

    @patch("mvp.atptour.cli.MatchesAggregator")
    @patch("mvp.atptour.cli.run_player_data")
    @patch("mvp.atptour.cli._process_tournaments")
    @patch("mvp.atptour.cli._resolve_backfill_tournaments")
    @patch("mvp.atptour.cli.run_rankings")
    def test_backfill_skips_aggregation(
        self,
        MockRankings,
        MockResolve,
        MockProcess,
        MockPlayerData,
        MockMatchesAgg,
    ):
        from mvp.atptour.cli import cmd_backfill

        MockResolve.return_value = []
        MockProcess.return_value = []
        MockPlayerData.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(year=2023, tid=None, circuit=None)
        cmd_backfill(args)

        MockMatchesAgg.assert_not_called()


class TestCmdModel:
    @patch("mvp.atptour.cli.MatchesAggregator")
    def test_model_runs_aggregation(self, MockMatchesAgg):
        from mvp.atptour.cli import cmd_model

        args = SimpleNamespace(config=None)
        cmd_model(args)

        MockMatchesAgg.return_value.run.assert_called_once()
