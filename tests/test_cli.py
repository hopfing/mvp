"""Tests for unified CLI."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestParseArgs:
    def test_no_subcommand_exits(self):
        from mvp.cli import parse_args

        with pytest.raises(SystemExit):
            parse_args([])

    def test_model_subcommand(self):
        from mvp.cli import parse_args

        args = parse_args(["model", "experiments/config.yaml"])
        assert args.command == "model"
        assert args.config == "experiments/config.yaml"

    def test_model_with_optional_args(self):
        from mvp.cli import parse_args

        args = parse_args([
            "model", "config.yaml",
            "--matches", "data/matches.parquet",
            "--mlflow-dir", "mlruns",
        ])
        assert args.matches == "data/matches.parquet"
        assert args.mlflow_dir == "mlruns"

    def test_live_subcommand(self):
        from mvp.cli import parse_args

        args = parse_args(["live"])
        assert args.command == "live"
        assert args.tid is None
        assert args.refresh is False

    def test_live_with_tid(self):
        from mvp.cli import parse_args

        args = parse_args(["live", "--tid", "580"])
        assert args.tid == "580"

    def test_live_with_refresh(self):
        from mvp.cli import parse_args

        args = parse_args(["live", "--refresh"])
        assert args.refresh is True

    def test_log_level_option(self):
        from mvp.cli import parse_args

        args = parse_args(["--log-level", "DEBUG", "live"])
        assert args.log_level == "DEBUG"


class TestCmdModel:
    def test_model_calls_runner(self, tmp_path: Path):
        from mvp.cli import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text("""
name: test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
""")

        with patch("mvp.model.runner.ExperimentRunner") as mock_runner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {
                "metrics": {"accuracy": 0.65, "log_loss": 0.68},
                "run_id": "abc123",
                "n_folds": 5,
            }
            mock_instance.config.name = "test"
            mock_runner.return_value = mock_instance

            result = main(["model", str(config_path)])

        mock_runner.assert_called_once()
        assert result == 0

    def test_model_missing_config_raises(self):
        from mvp.cli import main

        with pytest.raises(FileNotFoundError):
            main(["model", "/nonexistent/config.yaml"])


class TestCmdLive:
    @patch("mvp.atptour.aggregators.matches.MatchesAggregator")
    @patch("mvp.atptour.pipeline.run_player_data")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_full_chain(
        self,
        mock_rankings,
        mock_discovery,
        mock_process,
        mock_player_data,
        mock_aggregator,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [("580", 2026)]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid=None, refresh=False)
        result = cmd_live(args)

        mock_rankings.assert_called_once()
        mock_discovery.return_value.get_active_tournaments.assert_called_once()
        mock_process.assert_called_once()
        mock_player_data.assert_called_once()
        mock_aggregator.return_value.run.assert_called_once()
        assert result == 0

    @patch("mvp.atptour.aggregators.matches.MatchesAggregator")
    @patch("mvp.atptour.pipeline.run_player_data")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_with_tid_filters(
        self,
        mock_rankings,
        mock_discovery,
        mock_process,
        mock_player_data,
        mock_aggregator,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [
            ("580", 2026),
            ("339", 2026),
        ]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid="580", refresh=False)
        result = cmd_live(args)

        # Should only process the filtered tournament
        call_args = mock_process.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0][0] == "580"

    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_tid_not_found_raises(self, mock_rankings, mock_discovery):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [("580", 2026)]

        args = SimpleNamespace(tid="999", refresh=False)
        with pytest.raises(ValueError, match="not currently active"):
            cmd_live(args)

    @patch("mvp.atptour.aggregators.matches.MatchesAggregator")
    @patch("mvp.atptour.pipeline.run_player_data")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_raises_on_failures(
        self,
        mock_rankings,
        mock_discovery,
        mock_process,
        mock_player_data,
        mock_aggregator,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [("580", 2026)]
        mock_process.return_value = [("580", 2026, "boom")]
        mock_player_data.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid=None, refresh=False)
        with pytest.raises(RuntimeError, match="failed tournament"):
            cmd_live(args)
