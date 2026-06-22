"""Tests for stats_plus transformer (normalized long format)."""

import json

import polars as pl
import pytest

from mvp.atptour.schemas.stats_plus import SCHEMA_HASH, STAT_REGISTRY
from mvp.atptour.tournament import Tournament
from mvp.atptour.transformers.stats_plus import StatsPlusTransformer
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


def _stat_rows(stats: dict[str, tuple[str, str]], influence: str = "0%") -> list[dict]:
    """Build a list of stat rows from {stat name: (player1, player2)}."""
    return [
        {
            "name": name,
            "player1": p1,
            "player2": p2,
            "player1Bar": 0,
            "influence": influence,
            "player1Points": [],
            "player1CrucialPoints": [],
        }
        for name, (p1, p2) in stats.items()
    ]


def _make_match_data(
    set_stats=None,
    sets_completed=2,
    is_doubles=False,
    match_completed=True,
    players=None,
):
    """Create a full stats_plus JSON structure.

    `set_stats` maps a set key ("set0".."set5") to a {stat: (p1, p2)} dict.
    Defaults to CORE_STATS in set0, with empty set1/set2.
    """
    if set_stats is None:
        set_stats = {"set0": CORE_STATS, "set1": {}, "set2": {}}
    if players is None:
        players = [
            {"seed": "1", "player1Name": "P. One", "player1Id": "A123",
             "player1Country": "USA"},
            {"seed": "2", "player1Name": "P. Two", "player1Id": "B456",
             "player1Country": "GBR"},
        ]
    return {
        "courtId": 1,
        "matchCompleted": match_completed,
        "isDoubles": is_doubles,
        "setsCompleted": sets_completed,
        "players": players,
        "setStats": {k: _stat_rows(v) for k, v in set_stats.items()},
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


def _cell(df: pl.DataFrame, set_num: int, stat_key: str) -> dict:
    """Return the single row for (set_num, stat_key) as a dict."""
    sub = df.filter(
        (pl.col("set_num") == set_num) & (pl.col("stat_key") == stat_key)
    )
    assert len(sub) == 1, (
        f"expected one row for set {set_num}/{stat_key}, got {len(sub)}"
    )
    return sub.row(0, named=True)


class TestStatsPlusTransformer:
    def test_basic_transform(self, tmp_path, tournament):
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data())
        output = _run(tmp_path, tournament)

        assert output.exists()
        df = pl.read_parquet(output)
        # 16 core stats, set0 only (set1/set2 are empty -> no rows).
        assert len(df) == len(CORE_STATS)
        assert df["set_num"].unique().to_list() == [0]

        ctx = df.row(0, named=True)
        assert ctx["tournament_id"] == "339"
        assert ctx["year"] == 2023
        assert ctx["match_id"] == "MS001"
        assert ctx["p1_id"] == "A123"
        assert ctx["p2_id"] == "B456"
        assert ctx["sets_completed"] == 2

        # num stat: value in num, den null
        aces = _cell(df, 0, "svc_aces")
        assert aces["p1_num"] == 12 and aces["p1_den"] is None
        assert aces["p2_num"] == 13
        assert _cell(df, 0, "svc_serve_rating")["p1_num"] == 285

        # frac stat: num/den split
        fs = _cell(df, 0, "svc_first_serve")
        assert (fs["p1_num"], fs["p1_den"]) == (42, 73)
        assert (fs["p2_num"], fs["p2_den"]) == (48, 75)
        tp = _cell(df, 0, "pts_total_pts_won")
        assert (tp["p1_num"], tp["p1_den"]) == (71, 148)

    def test_zero_over_zero_is_not_null(self, tmp_path, tournament):
        """A present '0/0 (0%)' must stage as (0, 0), distinct from absent->null."""
        stats = dict(CORE_STATS)
        stats["Break Points Saved"] = ("0/0 (0%)", "3/3 (100%)")
        _write_json(
            tmp_path, tournament, "MS001.json",
            _make_match_data({"set0": stats}),
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        bp = _cell(df, 0, "svc_bp_saved")
        assert (bp["p1_num"], bp["p1_den"]) == (0, 0)
        assert (bp["p2_num"], bp["p2_den"]) == (3, 3)

    def test_absent_stats_produce_no_rows(self, tmp_path, tournament):
        """16-row matches lack net/winner/error/speed stats -> no rows for them."""
        _write_json(
            tmp_path, tournament, "MS001.json",
            _make_match_data({"set0": CORE_STATS}),
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        absent_keys = (
            "pts_net_pts_won",
            "winners",
            "unforced_errors",
            "max_serve_speed_kmh",
        )
        for absent in absent_keys:
            assert df.filter(pl.col("stat_key") == absent).is_empty()

    def test_extended_and_speed_stats(self, tmp_path, tournament):
        stats = {**CORE_STATS, **EXTENDED_STATS, **SPEED_STATS}
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data({"set0": stats})
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        net = _cell(df, 0, "pts_net_pts_won")
        assert (net["p1_num"], net["p1_den"]) == (8, 11)
        assert _cell(df, 0, "winners")["p1_num"] == 36
        assert _cell(df, 0, "unforced_errors")["p2_num"] == 30
        assert _cell(df, 0, "max_serve_speed_kmh")["p1_num"] == 210
        assert _cell(df, 0, "second_serve_avg_speed_kmh")["p2_num"] == 160

    def test_per_set_rows(self, tmp_path, tournament):
        """Per-set rows carry the value from *that* set (not set0's) — proves
        per-set value isolation, not just that the rows exist."""
        set1 = {**CORE_STATS, "Aces": ("4", "5")}
        set2 = {**CORE_STATS, "Aces": ("8", "8")}
        _write_json(
            tmp_path, tournament, "MS001.json",
            _make_match_data({"set0": CORE_STATS, "set1": set1, "set2": set2}),
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert sorted(df["set_num"].unique().to_list()) == [0, 1, 2]
        assert len(df) == 3 * len(CORE_STATS)
        assert _cell(df, 0, "svc_aces")["p1_num"] == 12
        assert _cell(df, 1, "svc_aces")["p1_num"] == 4
        assert _cell(df, 2, "svc_aces")["p1_num"] == 8

    def test_non_contiguous_sets(self, tmp_path, tournament):
        """A retirement leaves only set0/set1 present; iterate keys, not a range."""
        _write_json(
            tmp_path, tournament, "MS001.json",
            _make_match_data(
                {"set0": CORE_STATS, "set1": CORE_STATS}, sets_completed=1
            ),
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert sorted(df["set_num"].unique().to_list()) == [0, 1]

    def test_negative_value_treated_as_missing(self, tmp_path, tournament):
        """The feed emits -1 as a 'not tracked' sentinel; it must stage as null."""
        stats = {**CORE_STATS, **EXTENDED_STATS}
        stats["Unforced Errors"] = ("-1", "12")
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data({"set0": stats})
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        ue = _cell(df, 0, "unforced_errors")
        assert ue["p1_num"] is None
        assert ue["p2_num"] == 12

    def test_malformed_value_nulls_only_that_cell(self, tmp_path, tournament):
        """An unparseable value nulls that one cell; the match is still staged."""
        stats = dict(CORE_STATS)
        stats["Aces"] = ("N/A", "13")
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data({"set0": stats})
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert len(df) == len(CORE_STATS)  # match not dropped
        aces = _cell(df, 0, "svc_aces")
        assert aces["p1_num"] is None
        assert aces["p2_num"] == 13
        # other stats unaffected
        assert _cell(df, 0, "svc_serve_rating")["p1_num"] == 285

    def test_influence_parsed_to_fraction(self, tmp_path, tournament):
        data = _make_match_data({"set0": CORE_STATS})
        # override influence on every set0 row to a non-zero value
        for row in data["setStats"]["set0"]:
            row["influence"] = "45%"
        _write_json(tmp_path, tournament, "MS001.json", data)
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert _cell(df, 0, "svc_aces")["influence"] == pytest.approx(0.45)

    def test_influence_unparseable_is_null(self, tmp_path, tournament):
        data = _make_match_data({"set0": CORE_STATS})
        for row in data["setStats"]["set0"]:
            row["influence"] = "N/A"
        _write_json(tmp_path, tournament, "MS001.json", data)
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert _cell(df, 0, "svc_aces")["influence"] is None

    def test_influence_numeric_is_null(self, tmp_path, tournament):
        """A bare numeric influence (unknown unit) stages as null, not a guess."""
        data = _make_match_data({"set0": CORE_STATS})
        for row in data["setStats"]["set0"]:
            row["influence"] = 6  # numeric, not the observed "6%" string
        _write_json(tmp_path, tournament, "MS001.json", data)
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert _cell(df, 0, "svc_aces")["influence"] is None

    def test_nameless_stat_skipped(self, tmp_path, tournament):
        """A stat row with no `name` key is skipped (not staged, no raise)."""
        data = _make_match_data({"set0": CORE_STATS})
        data["setStats"]["set0"].append(
            {"player1": "9", "player2": "9", "influence": "0%"}
        )
        _write_json(tmp_path, tournament, "MS001.json", data)
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert len(df) == len(CORE_STATS)

    def test_unmapped_sr_id_skips_file(self, tmp_path, tournament):
        """An unmapped Sportradar player ID fails hard (ADR-002) and drops the
        match's file rather than crashing the run."""
        data = _make_match_data()
        data["players"][0]["player1Id"] = "SR:COMPETITOR:99999"
        _write_json(tmp_path, tournament, "MS001.json", data)
        assert not _run(tmp_path, tournament).exists()

    def test_unknown_stat_skipped(self, tmp_path, tournament):
        """A stat name not in the registry is skipped, not staged or raised."""
        data = _make_match_data({"set0": CORE_STATS})
        data["setStats"]["set0"].append(
            {"name": "Mystery Stat", "player1": "9", "player2": "9", "influence": "0%"}
        )
        _write_json(tmp_path, tournament, "MS001.json", data)
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert df.filter(pl.col("stat_name") == "Mystery Stat").is_empty()
        assert len(df) == len(CORE_STATS)

    def test_value_columns_typed_int64(self, tmp_path, tournament):
        """num/den columns must be Int64 (via schema_overrides) even when a
        column is entirely null, so downstream concat/join doesn't break."""
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data({"set0": CORE_STATS})
        )
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert df.schema["p1_num"] == pl.Int64
        assert df.schema["p1_den"] == pl.Int64
        assert df.schema["influence"] == pl.Float64
        # num stats leave den null -> all-null den column still Int64
        aces_den = df.filter(pl.col("stat_key") == "svc_aces")["p1_den"]
        assert aces_den.null_count() == len(aces_den)

    def test_skips_incomplete_match(self, tmp_path, tournament):
        _write_json(
            tmp_path, tournament, "MS001.json", _make_match_data(match_completed=False)
        )
        assert not _run(tmp_path, tournament).exists()

    def test_skips_empty_set_stats(self, tmp_path, tournament):
        data = _make_match_data()
        data["setStats"] = {}
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
        df = pl.read_parquet(_run(tmp_path, tournament))
        ctx = df.row(0, named=True)
        assert ctx["p1_id"] == "A123"
        assert ctx["p2_id"] == ""

    def test_multiple_matches(self, tmp_path, tournament):
        _write_json(tmp_path, tournament, "MS001.json", _make_match_data())
        _write_json(tmp_path, tournament, "MS002.json", _make_match_data())
        df = pl.read_parquet(_run(tmp_path, tournament))
        assert df["match_id"].unique().sort().to_list() == ["MS001", "MS002"]
        assert len(df) == 2 * len(CORE_STATS)

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
        df = pl.read_parquet(_run(tmp_path, tournament))
        ctx = df.row(0, named=True)
        assert ctx["p1_id"] == "A123"
        assert ctx["p2_id"] == "B456"


def test_registry_keys_are_unique_and_kinds_valid():
    """Each stat maps to a unique key with a valid kind."""
    keys = [k for k, _ in STAT_REGISTRY.values()]
    assert len(keys) == len(set(keys)), "duplicate stat_key in STAT_REGISTRY"
    assert all(kind in ("frac", "num") for _, kind in STAT_REGISTRY.values())
