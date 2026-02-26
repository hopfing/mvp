"""Tests for CLI subcommand argument parsing."""

from __future__ import annotations

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
