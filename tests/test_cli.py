"""Tests for unified CLI."""


from datetime import datetime
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

    def test_model_basic(self):
        from mvp.cli import parse_args

        args = parse_args(["model", "baseline"])
        assert args.command == "model"
        assert args.config == "baseline"

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

    def test_train_subcommand(self):
        from mvp.cli import parse_args

        args = parse_args(["train"])
        assert args.command == "train"

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
            mock_instance.run_name = "test"
            mock_runner.return_value = mock_instance

            result = main(["model", str(config_path)])

        mock_runner.assert_called_once()
        assert result == 0

    def test_model_missing_config_raises(self):
        from mvp.cli import main

        with pytest.raises(FileNotFoundError):
            main(["model", "/nonexistent/config.yaml"])


class TestCmdTrain:
    @patch("mvp.model.predictor.ProductionPredictor")
    def test_train_calls_predictor(self, mock_predictor_cls):
        from mvp.cli import cmd_train

        args = SimpleNamespace()
        result = cmd_train(args)

        mock_predictor_cls.return_value.train.assert_called_once()
        assert result == 0


class TestCmdLive:
    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
    @patch("mvp.model.predictor.ProductionPredictor")
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
        mock_predictor_cls,
        mock_dk,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [
            ("580", 2026)
        ]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)
        mock_predictor_cls.return_value.predict.return_value = MagicMock(__len__=lambda s: 0)

        args = SimpleNamespace(tid=None, refresh=False, refresh_players=False)
        result = cmd_live(args)

        mock_rankings.assert_called_once()
        mock_discovery.return_value.get_active_tournaments.assert_called_once()
        mock_process.assert_called_once()
        mock_player_data.assert_called_once()
        mock_aggregator.return_value.run.assert_called_once()
        mock_predictor_cls.return_value.predict.assert_called_once()
        assert result == 0

    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
    @patch("mvp.model.predictor.ProductionPredictor")
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
        mock_predictor_cls,
        mock_dk,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [
            ("580", 2026),
            ("339", 2026),
        ]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)
        mock_predictor_cls.return_value.predict.return_value = MagicMock(__len__=lambda s: 0)

        args = SimpleNamespace(tid="580", refresh=False, refresh_players=False)
        cmd_live(args)

        # Should only process the filtered tournament
        call_args = mock_process.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0][0] == "580"

    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_tid_not_found_raises(self, mock_rankings, mock_discovery, mock_dk):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [
            ("580", 2026)
        ]

        args = SimpleNamespace(tid="999", refresh=False, refresh_players=False)
        with pytest.raises(ValueError, match="not currently active"):
            cmd_live(args)

    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
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
        mock_dk,
    ):
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [
            ("580", 2026)
        ]
        mock_process.return_value = [("580", 2026, "boom")]
        mock_player_data.return_value = MagicMock(has_failures=False)

        args = SimpleNamespace(tid=None, refresh=False, refresh_players=False)
        with pytest.raises(RuntimeError, match="failed tournament"):
            cmd_live(args)


class TestCmdLiveSheets:
    """Tests for Sheets sync integration in cmd_live."""

    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
    @patch("mvp.gsheets.sheets.SheetsSync")
    @patch("mvp.gsheets.base.merge_predictions")
    @patch("mvp.gsheets.base.prepare_predictions")
    @patch("mvp.model.predictor.ProductionPredictor")
    @patch("mvp.atptour.aggregators.matches.MatchesAggregator")
    @patch("mvp.atptour.pipeline.run_player_data")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_pushes_to_sheets(
        self,
        mock_rankings,
        mock_discovery,
        mock_process,
        mock_player_data,
        mock_aggregator,
        mock_predictor_cls,
        mock_prepare,
        mock_merge,
        mock_sheets_cls,
        mock_dk,
    ):
        """Sheets sync is called after predictions are saved."""
        import polars as pl
        from mvp.cli import cmd_live
        from mvp.gsheets.base import COLUMN_NAMES

        mock_discovery.return_value.get_active_tournaments.return_value = [("580", 2026)]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)

        # Mock predictions as a real DataFrame (cmd_live checks len())
        predictions = pl.DataFrame({
            "match_uid": ["M1"],
            "p1_name": ["John"],
            "p2_name": ["Jane"],
            "p1_win_prob": [0.65],
            "p2_win_prob": [0.35],
            "p1_elo": [1530.0],
            "p2_elo": [1470.0],
            "tournament_name": ["Open"],
            "circuit": ["tour"],
            "surface": ["Hard"],
            "round": ["R32"],
            "effective_match_date": [datetime(2024, 1, 15)],
            "model_version": ["v1"],
            "predicted_at": [datetime(2024, 1, 14)],
        })
        mock_predictor_cls.return_value.predict.return_value = predictions

        # Mock Sheets
        mock_sheets = MagicMock()
        mock_sheets.read_existing.return_value = pl.DataFrame(
            schema={col: pl.Utf8 for col in COLUMN_NAMES}
        )
        mock_sheets_cls.return_value = mock_sheets
        mock_prepare.return_value = pl.DataFrame({"match_uid": ["M1"]})
        mock_merge.return_value = pl.DataFrame({"match_uid": ["M1"]})

        args = SimpleNamespace(tid=None, refresh=False, refresh_players=False)
        result = cmd_live(args)

        assert result == 0
        mock_sheets.write.assert_called_once()

    @patch("mvp.cli._fetch_dk_quiet", return_value=0)
    @patch("mvp.gsheets.sheets.SheetsSync")
    @patch("mvp.model.predictor.ProductionPredictor")
    @patch("mvp.atptour.aggregators.matches.MatchesAggregator")
    @patch("mvp.atptour.pipeline.run_player_data")
    @patch("mvp.atptour.pipeline._process_tournaments")
    @patch("mvp.atptour.discovery.TournamentDiscovery")
    @patch("mvp.atptour.pipeline.run_rankings")
    def test_live_continues_when_sheets_fails(
        self,
        mock_rankings,
        mock_discovery,
        mock_process,
        mock_player_data,
        mock_aggregator,
        mock_predictor_cls,
        mock_sheets_cls,
        mock_dk,
    ):
        """Pipeline completes even if Sheets sync raises."""
        import polars as pl
        from mvp.cli import cmd_live

        mock_discovery.return_value.get_active_tournaments.return_value = [("580", 2026)]
        mock_process.return_value = []
        mock_player_data.return_value = MagicMock(has_failures=False)

        predictions = pl.DataFrame({
            "match_uid": ["M1"],
            "p1_name": ["John"],
            "p2_name": ["Jane"],
            "p1_win_prob": [0.65],
            "p2_win_prob": [0.35],
            "p1_elo": [1530.0],
            "p2_elo": [1470.0],
            "tournament_name": ["Open"],
            "circuit": ["tour"],
            "surface": ["Hard"],
            "round": ["R32"],
            "effective_match_date": [datetime(2024, 1, 15)],
            "model_version": ["v1"],
            "predicted_at": [datetime(2024, 1, 14)],
        })
        mock_predictor_cls.return_value.predict.return_value = predictions

        # SheetsSync constructor raises
        mock_sheets_cls.side_effect = ValueError("No credentials")

        args = SimpleNamespace(tid=None, refresh=False, refresh_players=False)
        result = cmd_live(args)

        # Should succeed despite Sheets failure
        assert result == 0
