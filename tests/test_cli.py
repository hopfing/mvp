"""Tests for unified CLI."""


import json
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
        assert args.memory_limit is None  # defaults to no override

    def test_train_memory_limit(self):
        from mvp.cli import parse_args

        args = parse_args(["train", "--memory-limit", "0"])
        assert args.command == "train"
        assert args.memory_limit == 0  # 0 => guard disabled (bypass)

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
    @pytest.fixture(autouse=True)
    def _no_books_wait(self):
        # The books job is a separate process in production; tests stage no
        # sentinel, so skip cmd_live's bounded wait for it (avoids 120s/test).
        with patch("mvp.cli._wait_for_books", return_value=True):
            yield

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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


class TestBookOutageAlert:
    """_alert_book_outages: onset-only alert after X consecutive 0-entry runs."""

    def _write_books_runs(self, data_root: Path, br_counts: list[int]) -> None:
        path = data_root / "pipeline" / "runs.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for c in br_counts:
                f.write(
                    json.dumps({"job": "books", "books_fetched": {"br": c}}) + "\n"
                )

    def _br_book(self):
        from mvp.cli import BOOK_REGISTRY

        return [b for b in BOOK_REGISTRY if b.code == "br"]

    @patch("mvp.cli.notify.post_failure")
    def test_fires_on_outage_onset(self, mock_fail, tmp_path):
        from mvp.cli import _alert_book_outages

        # last good 4 runs ago; this run makes the 4th consecutive zero
        self._write_books_runs(tmp_path, [5, 0, 0, 0])
        _alert_book_outages(tmp_path, {"br": 0}, self._br_book())
        mock_fail.assert_called_once()

    @patch("mvp.cli.notify.post_failure")
    def test_ignores_transient_blip(self, mock_fail, tmp_path):
        from mvp.cli import _alert_book_outages

        self._write_books_runs(tmp_path, [5, 5, 5, 5])
        _alert_book_outages(tmp_path, {"br": 0}, self._br_book())  # single 0
        mock_fail.assert_not_called()

    @patch("mvp.cli.notify.post_failure")
    def test_no_realert_when_already_down(self, mock_fail, tmp_path):
        from mvp.cli import _alert_book_outages

        self._write_books_runs(tmp_path, [0, 0, 0, 0])
        _alert_book_outages(tmp_path, {"br": 0}, self._br_book())
        mock_fail.assert_not_called()

    @patch("mvp.cli.notify.post_failure")
    def test_no_alert_without_enough_history(self, mock_fail, tmp_path):
        from mvp.cli import _alert_book_outages

        self._write_books_runs(tmp_path, [0, 0])
        _alert_book_outages(tmp_path, {"br": 0}, self._br_book())
        mock_fail.assert_not_called()


class TestCmdLiveSheets:
    """Tests for Sheets sync integration in cmd_live."""

    @pytest.fixture(autouse=True)
    def _no_books_wait(self):
        with patch("mvp.cli._wait_for_books", return_value=True):
            yield

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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
        mock_predictor_cls.return_value.predict_voters.return_value = predictions

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

    @patch("mvp.cli._fetch_book_quiet", return_value=0)
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


_FS_CONFIG = """\
description: a run
data:
  date_range:
    start: "2021-01-01"
    end: "2025-12-31"
features:
  include:
    - win_rate(days=30)
discovery:
  metric: log_loss
model:
  type: xgboost
validation:
  type: date_sliding
  train_months: 12
  test_months: 3
"""


class TestExperimentConfigOptional:
    def test_config_optional_with_resume(self):
        from mvp.cli import parse_args

        args = parse_args(["experiment", "-o", "run1", "--resume"])
        assert args.config is None
        assert args.resume is True

    def test_config_positional_still_parses(self):
        from mvp.cli import parse_args

        args = parse_args(["experiment", "xgb_ds.yaml", "-o", "run1"])
        assert args.config == "xgb_ds.yaml"


class TestResolveRunConfig:
    def _write(self, path: Path, text: str = _FS_CONFIG) -> Path:
        path.write_text(text)
        return path

    def _snapshot(self, run_dir: Path) -> Path:
        # Deterministic snapshot path: named after the run, not the input config.
        return run_dir / f"{run_dir.name}_experiment.yaml"

    def test_fresh_snapshots_config_into_run_dir(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        src = self._write(tmp_path / "xgb_ds.yaml")
        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        args = SimpleNamespace(resume=False, config=str(src))

        resolved = _resolve_run_config(args, run_dir)

        # Loads from a frozen copy named after the run, byte-identical to source.
        assert resolved == run_dir / "run1_experiment.yaml"
        assert resolved.exists()
        assert resolved.read_text() == _FS_CONFIG

    def test_fresh_snapshot_preserves_key_order(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        # A deliberately non-alphabetical key order that a yaml re-dump would sort.
        text = "model:\n  type: xgboost\ndata:\n  date_range:\n    start: 1\n"
        src = self._write(tmp_path / "xgb_ds.yaml", text)
        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        args = SimpleNamespace(resume=False, config=str(src))

        resolved = _resolve_run_config(args, run_dir)

        assert resolved.read_text() == text  # verbatim: 'model' still before 'data'

    def test_fresh_requires_config(self, tmp_path: Path, capsys):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        args = SimpleNamespace(resume=False, config=None)

        assert _resolve_run_config(args, run_dir) is None
        assert "required for a fresh run" in capsys.readouterr().out

    def test_fresh_missing_config_errors(self, tmp_path: Path, capsys):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        args = SimpleNamespace(resume=False, config=str(tmp_path / "nope.yaml"))

        assert _resolve_run_config(args, run_dir) is None
        assert "not found" in capsys.readouterr().out

    def test_fresh_overwrites_existing_snapshot(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        self._snapshot(run_dir).write_text("stale: true\n")
        src = self._write(tmp_path / "xgb_ds.yaml")
        args = SimpleNamespace(resume=False, config=str(src))

        resolved = _resolve_run_config(args, run_dir)

        assert resolved == self._snapshot(run_dir)
        assert resolved.read_text() == _FS_CONFIG

    def test_resume_loads_snapshot_ignoring_positional(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        snapshot = self._write(self._snapshot(run_dir))
        # A different config passed on resume must not be what loads.
        other = self._write(
            tmp_path / "other.yaml", _FS_CONFIG.replace("win_rate(days=30)", "elo_diff")
        )
        args = SimpleNamespace(resume=True, config=str(other))

        assert _resolve_run_config(args, run_dir) == snapshot

    def test_resume_without_positional(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        snapshot = self._write(self._snapshot(run_dir))
        args = SimpleNamespace(resume=True, config=None)

        assert _resolve_run_config(args, run_dir) == snapshot

    def test_resume_warns_on_drift(self, tmp_path: Path, caplog):
        import logging

        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        self._write(self._snapshot(run_dir))
        drifted = self._write(
            tmp_path / "xgb_ds.yaml",
            _FS_CONFIG.replace("win_rate(days=30)", "elo_diff"),
        )
        args = SimpleNamespace(resume=True, config=str(drifted))

        with caplog.at_level(logging.WARNING):
            _resolve_run_config(args, run_dir)

        assert any("Config drift" in r.message for r in caplog.records)

    def test_resume_legacy_adopts_positional_as_snapshot(self, tmp_path: Path):
        from mvp.cli import _resolve_run_config

        # No snapshot in the run dir (a run started before snapshots existed).
        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        src = self._write(tmp_path / "xgb_ds.yaml")
        args = SimpleNamespace(resume=True, config=str(src))

        resolved = _resolve_run_config(args, run_dir)

        # Adopts the positional config as the snapshot and loads from it.
        assert resolved == self._snapshot(run_dir)
        assert resolved.exists()
        assert resolved.read_text() == _FS_CONFIG

    def test_resume_legacy_without_config_errors(self, tmp_path: Path, capsys):
        from mvp.cli import _resolve_run_config

        run_dir = tmp_path / "fs_runs" / "run1"
        run_dir.mkdir(parents=True)
        args = SimpleNamespace(resume=True, config=None)

        assert _resolve_run_config(args, run_dir) is None
        assert "No config snapshot" in capsys.readouterr().out


class TestConfigDrift:
    def test_identical_configs_no_drift(self, tmp_path: Path):
        from mvp.cli import _config_drift

        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text(_FS_CONFIG)
        b.write_text(_FS_CONFIG)
        assert _config_drift(a, b) == []

    def test_feature_change_is_drift(self, tmp_path: Path):
        from mvp.cli import _config_drift

        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text(_FS_CONFIG)
        b.write_text(_FS_CONFIG.replace("win_rate(days=30)", "elo_diff"))
        assert _config_drift(a, b) == ["features"]

    def test_description_change_ignored(self, tmp_path: Path):
        from mvp.cli import _config_drift

        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text(_FS_CONFIG)
        b.write_text(_FS_CONFIG.replace("description: a run", "description: renamed"))
        assert _config_drift(a, b) == []

    def test_worker_knob_change_ignored(self, tmp_path: Path):
        from mvp.cli import _config_drift

        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text(_FS_CONFIG)
        b.write_text(_FS_CONFIG.replace(
            "  metric: log_loss", "  metric: log_loss\n  forward_max_workers: 4"
        ))
        assert _config_drift(a, b) == []
