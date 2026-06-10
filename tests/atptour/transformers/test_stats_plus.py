"""Tests for stats_plus transformer."""

import json

import polars as pl
import pytest

from mvp.atptour.schemas.match_stats import MatchStatsRecord
from mvp.atptour.schemas.stats_plus import SCHEMA_HASH, StatsPlusRecord
from mvp.atptour.transformers.stats_plus import StatsPlusTransformer
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="339",
        year=2023,
        circuit=Circuit.tour,
        location="Indian Wells, USA",
    )


# The 16 always-present stats (values lifted from a real file).
CORE_STATS = {
    "Serve Rating": ("285", "305"),
    "Aces": ("12", "13"),
    "Double Faults": ("8", "3"),
    "1st Serve": ("42/73 (58%)", "48/75 (64%)"),
    "1st Serve Points Won": ("35/42 (83%)", "42/48 (88%)"),
    "2nd Serve Points Won": ("16/31 (52%)", "13/27 (48%)"),
    "Break Points Saved": ("5/6 (83%)", "3/3 (100%)"),
    "Service Games Played": ("11", "11"),
    "Return Rating": ("64", "91"),
    "1st Serve Return Points Won": ("6/48 (13%)", "7/42 (17%)"),
    "2nd Serve Return Points Won": ("14/27 (52%)", "15/31 (48%)"),
    "Break Points Converted": ("0/3 (0%)", "1/6 (17%)"),
    "Return Games Played": ("11", "11"),
    "Service Points Won": ("51/73 (70%)", "55/75 (73%)"),
    "Return Points Won": ("20/75 (27%)", "22/73 (30%)"),
    "Total Points Won": ("71/148 (48%)", "77/148 (52%)"),
}

EXTENDED_STATS = {
    "Net Points Won": ("8/11 (73%)", "12/18 (67%)"),
    "Winners": ("36", "25"),
    "Unforced Errors": ("14", "30"),
}

SPEED_STATS = {
    "Max Speed": ("210", "205"),
    "1st Serve Average Speed": ("188", "192"),
    "2nd Serve Average Speed": ("152", "160"),
}


def _set0(stats: dict[str, tuple[str, str]]) -> list[dict]:
    """Build a set0 list from {stat name: (player1, player2)}."""
    return [
        {"name": name, "player1": p1, "player2": p2, "player1Bar": 0, "influence": "0%"}
        for name, (p1, p2) in stats.items()
    ]


def _make_match_data(
    stats=None, sets_completed=2, is_doubles=False, match_completed=True, players=None
):
    """Create a full stats_plus JSON structure with the given set0 stats."""
    if stats is None:
        stats = CORE_STATS
    if players is None:
        players = [
            {"seed": "1", "player1Name": "P. One", "player1Id": "A123", "player1Country": "USA"},
            {"seed": "2", "player1Name": "P. Two", "player1Id": "B456", "player1Country": "GBR"},
        ]
    return {
        "courtId": 1,
        "matchCompleted": match_completed,
        "isDoubles": is_doubles,
        "setsCompleted": sets_completed,
        "players": players,
        "setStats": {"set0": _set0(stats), "set1": [], "set2": []},
    }


def _write_json(tmp_path, tournament, filename, data):
    """Write JSON file in the expected raw directory structure."""
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "stats_plus"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / filename
    path.write_text(json.dumps(data))
    return path


def _run(tmp_path, tournament):
    StatsPlusTransformer(tournament, data_root=tmp_path).run()
    return tmp_path / "stage" / "atptour" / tournament.path / "stats_plus.parquet"


class TestStatsPlusTransformer:
    def test_basic_transform(self, tmp_path, tournament):
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data())
        output = _run(tmp_path, tournament)

        assert output.exists()
        df = pl.read_parquet(output)
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["tournament_id"] == "339"
        assert row["year"] == 2023
        assert row["match_id"] == "MS001"
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"
        assert row["sets_completed"] == 2
        # num
        assert row["p1_svc_aces"] == 12
        assert row["p2_svc_aces"] == 13
        assert row["p1_svc_serve_rating"] == 285
        # frac split (numerator/denominator), match_stats-aligned names
        assert row["p1_svc_first_serve_in"] == 42
        assert row["p1_svc_first_serve_att"] == 73
        assert row["p2_svc_first_serve_in"] == 48
        assert row["p2_svc_first_serve_att"] == 75
        assert row["p1_pts_total_pts_won"] == 71
        assert row["p1_pts_total_pts_played"] == 148

    def test_zero_over_zero_is_not_null(self, tmp_path, tournament):
        """A present '0/0 (0%)' must stage as (0, 0), distinct from absent->null."""
        stats = dict(CORE_STATS)
        stats["Break Points Saved"] = ("0/0 (0%)", "3/3 (100%)")
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(stats))
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)

        assert row["p1_svc_bp_saved"] == 0
        assert row["p1_svc_bp_faced"] == 0
        assert row["p2_svc_bp_saved"] == 3
        assert row["p2_svc_bp_faced"] == 3

    def test_absent_stats_are_null(self, tmp_path, tournament):
        """16-row matches lack net/winner/error/speed stats -> null, not 0."""
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(CORE_STATS))
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)

        assert row["p1_pts_net_pts_won"] is None
        assert row["p1_pts_net_pts_played"] is None
        assert row["p1_winners"] is None
        assert row["p1_unforced_errors"] is None
        assert row["p1_max_serve_speed_kmh"] is None

    def test_extended_and_speed_stats(self, tmp_path, tournament):
        stats = {**CORE_STATS, **EXTENDED_STATS, **SPEED_STATS}
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(stats))
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)

        assert row["p1_pts_net_pts_won"] == 8
        assert row["p1_pts_net_pts_played"] == 11
        assert row["p1_winners"] == 36
        assert row["p2_unforced_errors"] == 30
        assert row["p1_max_serve_speed_kmh"] == 210
        assert row["p2_second_serve_avg_speed_kmh"] == 160

    def test_all_null_tiered_columns_are_int64(self, tmp_path, tournament):
        """A tournament of only core-stat matches must still type tiered columns
        as Int64 (not pl.Null) so downstream concat/join doesn't break."""
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(CORE_STATS))
        _write_json(tmp_path, tournament, "MS002.json", _make_match_data(CORE_STATS))
        df = pl.read_parquet(_run(tmp_path, tournament))

        assert len(df) == 2
        assert df.schema["p1_pts_net_pts_won"] == pl.Int64
        assert df.schema["p1_max_serve_speed_kmh"] == pl.Int64
        assert df["p1_pts_net_pts_won"].null_count() == 2

    def test_negative_value_treated_as_missing(self, tmp_path, tournament):
        """The feed emits -1 as a 'not tracked' sentinel; it must stage as null."""
        stats = {**CORE_STATS, **EXTENDED_STATS}
        stats["Unforced Errors"] = ("-1", "12")
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(stats))
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)

        assert row["p1_unforced_errors"] is None
        assert row["p2_unforced_errors"] == 12

    def test_malformed_required_stat_skips_record(self, tmp_path, tournament):
        """An unparseable value on a required core stat drops that match record."""
        stats = dict(CORE_STATS)
        stats["Aces"] = ("N/A", "13")
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(stats))
        assert not _run(tmp_path, tournament).exists()

    def test_skips_incomplete_match(self, tmp_path, tournament):
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data(match_completed=False)
        )
        assert not _run(tmp_path, tournament).exists()

    def test_skips_empty_set0(self, tmp_path, tournament):
        data = _make_match_data()
        data["setStats"]["set0"] = []
        _write_json(tmp_path, tournament, "MS001.json", data)
        assert not _run(tmp_path, tournament).exists()

    def test_doubles_still_staged(self, tmp_path, tournament):
        """Doubles are staged here; filtering happens at the aggregator."""
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data(is_doubles=True)
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert df.row(0, named=True)["is_doubles"] is True

    def test_missing_player_falls_back_to_empty(self, tmp_path, tournament):
        """A players list with <2 entries leaves p2_id empty rather than raising."""
        one_player = [
            {"seed": "1", "player1Name": "P. One", "player1Id": "A123"},
        ]
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data(players=one_player)
        )
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == ""

    def test_multiple_matches(self, tmp_path, tournament):
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data())
        _write_json(tmp_path, tournament, "MS002.json", _make_match_data())
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert len(df) == 2

    def test_no_data_directory(self, tmp_path, tournament):
        StatsPlusTransformer(tournament, data_root=tmp_path).run()  # no raise

    def test_schema_hash_in_metadata(self, tmp_path, tournament):
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data())
        output = _run(tmp_path, tournament)
        job = StatsPlusTransformer(tournament, data_root=tmp_path)
        assert job.is_schema_current(output, SCHEMA_HASH)
        assert not job.is_schema_current(output, "not_the_hash")

    def test_player_ids_uppercased(self, tmp_path, tournament):
        data = _make_match_data()
        data["players"][0]["player1Id"] = "a123"
        data["players"][1]["player1Id"] = "b456"
        _write_json(tmp_path, tournament, "MS001.json", data)
        row = pl.read_parquet(_run(tmp_path, tournament)).row(0, named=True)
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"


def test_stat_columns_align_with_match_stats():
    """Backwards compat: every match_stats svc/ret/pts stat column must exist
    in stats_plus under the same name."""
    cats = ("svc", "ret", "pts")
    shared = {
        c
        for c in MatchStatsRecord.model_fields
        if any(c.startswith(f"p1_{cat}_") or c.startswith(f"p2_{cat}_") for cat in cats)
    }
    sp_fields = StatsPlusRecord.model_fields
    missing = shared - set(sp_fields)
    assert not missing, f"stats_plus missing match_stats stat columns: {sorted(missing)}"
    # Types must match too, or the columns aren't truly backwards-compatible.
    mismatched = {
        c
        for c in shared
        if sp_fields[c].annotation != MatchStatsRecord.model_fields[c].annotation
    }
    assert not mismatched, f"stat column types diverge from match_stats: {sorted(mismatched)}"
