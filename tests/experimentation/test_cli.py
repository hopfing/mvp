"""Tests for CLI."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestCLI:
    """Tests for CLI entry point."""

    def test_run_command_calls_runner(self, tmp_path: Path):
        """Run command invokes ExperimentRunner."""
        from mvp.experimentation.cli import main

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

        with patch("mvp.experimentation.cli.ExperimentRunner") as mock_runner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {
                "metrics": {"accuracy": 0.65, "log_loss": 0.68},
                "run_id": "abc123",
                "n_folds": 5,
            }
            mock_instance.config.name = "test"
            mock_runner.return_value = mock_instance

            result = main(["run", str(config_path)])

        mock_runner.assert_called_once()
        assert result == 0

    def test_run_command_with_optional_args(self, tmp_path: Path):
        """Run command passes optional arguments to runner."""
        from mvp.experimentation.cli import main

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
        matches_path = tmp_path / "matches.parquet"
        matches_path.touch()
        mlflow_dir = tmp_path / "mlruns"

        with patch("mvp.experimentation.cli.ExperimentRunner") as mock_runner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {
                "metrics": {"accuracy": 0.65},
                "run_id": "abc123",
                "n_folds": 3,
            }
            mock_instance.config.name = "test"
            mock_runner.return_value = mock_instance

            result = main([
                "run",
                str(config_path),
                "--matches", str(matches_path),
                "--mlflow-dir", str(mlflow_dir),
            ])

        call_kwargs = mock_runner.call_args.kwargs
        assert call_kwargs["config_path"] == config_path
        assert call_kwargs["matches_path"] == matches_path
        assert call_kwargs["mlflow_dir"] == mlflow_dir
        assert result == 0

    def test_run_command_missing_config(self):
        """Run command fails when config file does not exist."""
        from mvp.experimentation.cli import main

        with pytest.raises(FileNotFoundError):
            main(["run", "/nonexistent/config.yaml"])

    def test_no_command_shows_help(self, capsys):
        """No command shows error."""
        from mvp.experimentation.cli import main

        with pytest.raises(SystemExit) as exc_info:
            main([])

        # argparse exits with code 2 for missing required args
        assert exc_info.value.code == 2
