"""Tests for atptour CLI backfill subcommand."""


from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


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


class TestParseArgsShared:
    def test_log_level_before_subcommand(self):
        from mvp.atptour.cli import parse_args

        args = parse_args(["--log-level", "DEBUG", "backfill", "--year", "2023"])
        assert args.log_level == "DEBUG"
        assert args.command == "backfill"

    def test_invalid_log_level(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--log-level", "TRACE", "backfill", "--year", "2023"])

    def test_no_subcommand_exits(self):
        from mvp.atptour.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args([])


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

