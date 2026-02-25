"""Tests for aggregation helper functions."""

from datetime import date, datetime

import polars as pl

from mvp.atptour.aggregators.helpers import (
    explode_match_stats,
    explode_results,
    explode_schedule,
    explode_to_player_match,
)

# ---------------------------------------------------------------------------
# explode_to_player_match tests (existing)
# ---------------------------------------------------------------------------


def _make_match_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "match_uid": ["2023_580_SGL_F_AAAA_BBBB"],
            "p1_id": ["AAAA"],
            "p2_id": ["BBBB"],
            "p1_seed": [1],
            "p2_seed": [None],
        }
    )


PLAYER_COLS = {"p1_id": "player_id", "p1_seed": "player_seed"}
OPP_COLS = {"p2_id": "opp_id", "p2_seed": "opp_seed"}
SHARED_COLS = ["match_uid"]


def test_explode_produces_two_rows_per_match():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 2


def test_explode_player_ids():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    players = result.sort("player_id")
    assert players["player_id"].to_list() == ["AAAA", "BBBB"]
    assert players["opp_id"].to_list() == ["BBBB", "AAAA"]


def test_explode_swaps_player_indexed_fields():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    row_a = result.filter(pl.col("player_id") == "AAAA")
    row_b = result.filter(pl.col("player_id") == "BBBB")
    assert row_a["player_seed"].item() == 1
    assert row_a["opp_seed"].item() is None
    assert row_b["player_seed"].item() is None
    assert row_b["opp_seed"].item() == 1


def test_explode_preserves_shared_cols():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert result["match_uid"].to_list() == [
        "2023_580_SGL_F_AAAA_BBBB",
        "2023_580_SGL_F_AAAA_BBBB",
    ]


def test_explode_drops_original_p1_p2_cols():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert "p1_id" not in result.columns
    assert "p2_id" not in result.columns
    assert "p1_seed" not in result.columns
    assert "p2_seed" not in result.columns


def test_explode_empty_df():
    df = pl.DataFrame(
        {
            "match_uid": pl.Series([], dtype=pl.String),
            "p1_id": pl.Series([], dtype=pl.String),
            "p2_id": pl.Series([], dtype=pl.String),
            "p1_seed": pl.Series([], dtype=pl.Int64),
            "p2_seed": pl.Series([], dtype=pl.Int64),
        }
    )
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 0
    assert "player_id" in result.columns
    assert "opp_id" in result.columns


def test_explode_multiple_matches():
    df = pl.DataFrame(
        {
            "match_uid": ["UID1", "UID2"],
            "p1_id": ["AAAA", "CCCC"],
            "p2_id": ["BBBB", "DDDD"],
            "p1_seed": [1, 3],
            "p2_seed": [2, 4],
        }
    )
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 4


# ---------------------------------------------------------------------------
# explode_results tests
# ---------------------------------------------------------------------------


def _make_results_df(
    *,
    match_uid: str | None = "2023_580_SGL_F_AAAA_BBBB",
    winner_id: str = "AAAA",
) -> pl.DataFrame:
    """Minimal results DataFrame with all required columns."""
    return pl.DataFrame(
        {
            "match_uid": [match_uid],
            "tournament_id": ["580"],
            "year": [2023],
            "circuit": ["atp"],
            "draw_type": ["singles"],
            "round": ["Finals"],
            "match_id": ["ms001"],
            "winner_id": [winner_id],
            "result_type": ["completed"],
            "duration_seconds": [7200],
            "p1_id": ["AAAA"],
            "p1_name": ["Player A"],
            "p1_country": ["USA"],
            "p1_seed": [1],
            "p1_entry": [None],
            "p1_partner_id": [None],
            "p1_partner_name": [None],
            "p1_partner_country": [None],
            "p2_id": ["BBBB"],
            "p2_name": ["Player B"],
            "p2_country": ["GBR"],
            "p2_seed": [None],
            "p2_entry": ["Q"],
            "p2_partner_id": [None],
            "p2_partner_name": [None],
            "p2_partner_country": [None],
            "p1_set1_games": [6],
            "p1_set1_tiebreak": [None],
            "p1_set2_games": [3],
            "p1_set2_tiebreak": [None],
            "p1_set3_games": [7],
            "p1_set3_tiebreak": [7],
            "p1_set4_games": [None],
            "p1_set4_tiebreak": [None],
            "p1_set5_games": [None],
            "p1_set5_tiebreak": [None],
            "p2_set1_games": [4],
            "p2_set1_tiebreak": [None],
            "p2_set2_games": [6],
            "p2_set2_tiebreak": [None],
            "p2_set3_games": [6],
            "p2_set3_tiebreak": [3],
            "p2_set4_games": [None],
            "p2_set4_tiebreak": [None],
            "p2_set5_games": [None],
            "p2_set5_tiebreak": [None],
            "source_file": ["results_singles.html"],
            "parsed_at": [datetime(2026, 2, 25, 12, 0, 0)],
        }
    )


class TestExplodeResults:
    def test_produces_two_rows_per_match(self):
        result = explode_results(_make_results_df())
        assert len(result) == 2

    def test_won_true_for_winner(self):
        result = explode_results(_make_results_df(winner_id="AAAA"))
        row_a = result.filter(pl.col("player_id") == "AAAA")
        assert row_a["won"].item() is True

    def test_won_false_for_loser(self):
        result = explode_results(_make_results_df(winner_id="AAAA"))
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_b["won"].item() is False

    def test_drops_name_and_country_columns(self):
        result = explode_results(_make_results_df())
        for col in [
            "p1_name", "p2_name", "p1_country", "p2_country",
            "p1_partner_name", "p1_partner_country",
            "p2_partner_name", "p2_partner_country",
        ]:
            assert col not in result.columns

    def test_drops_traceability_columns(self):
        result = explode_results(_make_results_df())
        assert "source_file" not in result.columns
        assert "parsed_at" not in result.columns

    def test_drops_winner_id(self):
        result = explode_results(_make_results_df())
        assert "winner_id" not in result.columns

    def test_set_scores_swap_correctly(self):
        result = explode_results(_make_results_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        # AAAA was p1, so AAAA's player scores = p1 scores
        assert row_a["player_set1_games"].item() == 6
        assert row_a["opp_set1_games"].item() == 4
        # BBBB was p2, so BBBB's player scores = p2 scores
        assert row_b["player_set1_games"].item() == 4
        assert row_b["opp_set1_games"].item() == 6

    def test_tiebreak_scores_swap(self):
        result = explode_results(_make_results_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["player_set3_tiebreak"].item() == 7
        assert row_a["opp_set3_tiebreak"].item() == 3
        assert row_b["player_set3_tiebreak"].item() == 3
        assert row_b["opp_set3_tiebreak"].item() == 7

    def test_seed_and_entry_swap(self):
        result = explode_results(_make_results_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["player_seed"].item() == 1
        assert row_a["opp_seed"].item() is None
        assert row_a["player_entry"].item() is None
        assert row_a["opp_entry"].item() == "Q"
        assert row_b["player_seed"].item() is None
        assert row_b["opp_seed"].item() == 1
        assert row_b["player_entry"].item() == "Q"
        assert row_b["opp_entry"].item() is None

    def test_partner_id_swap(self):
        df = _make_results_df()
        df = df.with_columns(
            pl.lit("CCCC").alias("p1_partner_id"),
            pl.lit("DDDD").alias("p2_partner_id"),
        )
        result = explode_results(df)
        row_a = result.filter(pl.col("player_id") == "AAAA")
        assert row_a["player_partner_id"].item() == "CCCC"
        assert row_a["opp_partner_id"].item() == "DDDD"

    def test_shared_cols_preserved(self):
        result = explode_results(_make_results_df())
        expected_shared = [
            "match_uid", "tournament_id", "year", "circuit",
            "draw_type", "round", "match_id", "result_type",
            "duration_seconds",
        ]
        for col in expected_shared:
            assert col in result.columns, f"Missing shared column: {col}"

    def test_drops_null_match_uid(self):
        df = _make_results_df(match_uid=None)
        result = explode_results(df)
        assert len(result) == 0

    def test_empty_dataframe(self):
        df = _make_results_df()
        empty = df.clear()
        result = explode_results(empty)
        assert len(result) == 0
        assert "player_id" in result.columns
        assert "won" in result.columns

    def test_multiple_matches(self):
        df1 = _make_results_df()
        df2 = df1.with_columns(
            pl.lit("UID2").alias("match_uid"),
            pl.lit("CCCC").alias("p1_id"),
            pl.lit("DDDD").alias("p2_id"),
            pl.lit("CCCC").alias("winner_id"),
        )
        combined = pl.concat([df1, df2])
        result = explode_results(combined)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# explode_match_stats tests
# ---------------------------------------------------------------------------

# All 26 stat fields that must be mapped for each player
_SVC_STAT_NAMES = [
    "svc_aces", "svc_double_faults",
    "svc_first_serve_in", "svc_first_serve_att",
    "svc_first_serve_pts_won", "svc_first_serve_pts_played",
    "svc_second_serve_pts_won", "svc_second_serve_pts_played",
    "svc_bp_saved", "svc_bp_faced",
    "svc_games_played", "svc_serve_rating",
]
_RET_STAT_NAMES = [
    "ret_first_serve_pts_won", "ret_first_serve_pts_played",
    "ret_second_serve_pts_won", "ret_second_serve_pts_played",
    "ret_bp_converted", "ret_bp_opportunities",
    "ret_games_played", "ret_return_rating",
]
_PTS_STAT_NAMES = [
    "pts_service_pts_won", "pts_service_pts_played",
    "pts_return_pts_won", "pts_return_pts_played",
    "pts_total_pts_won", "pts_total_pts_played",
]
_ALL_STAT_NAMES = _SVC_STAT_NAMES + _RET_STAT_NAMES + _PTS_STAT_NAMES


def _make_match_stats_df(
    *,
    match_uid: str | None = "2023_580_SGL_F_AAAA_BBBB",
    winner_id: str | None = "AAAA",
) -> pl.DataFrame:
    """Minimal match stats DataFrame with all required columns."""
    data: dict = {
        "match_uid": [match_uid],
        "tournament_id": ["580"],
        "year": [2023],
        "circuit": ["atp"],
        "draw_type": ["singles"],
        "round": ["Finals"],
        "round_id": [12],
        "match_id": ["ms001"],
        "surface": ["Hard"],
        "tournament_start_date": [date(2023, 1, 1)],
        "tournament_end_date": [date(2023, 1, 7)],
        "tournament_city": ["Melbourne"],
        "prize_money": [1000000],
        "currency": ["USD"],
        "draw_size_singles": [128],
        "draw_size_doubles": [64],
        "winner_id": [winner_id],
        "duration_seconds": [7200],
        "reason": [None],
        "number_of_sets": [3],
        "sets_played": [3],
        "is_qualifier": [False],
        "scoring_system": ["best_of_3"],
        "court_name": ["Centre Court"],
        "umpire_first_name": ["John"],
        "umpire_last_name": ["Smith"],
        "p1_id": ["AAAA"],
        "p2_id": ["BBBB"],
        "p1_partner_id": [None],
        "p2_partner_id": [None],
        "p1_seed": [1],
        "p1_entry": [None],
        "p2_seed": [None],
        "p2_entry": ["Q"],
        "source_file": ["ms001.json"],
        "parsed_at": [datetime(2026, 2, 25, 12, 0, 0)],
    }
    # P1 stats: use index+10 for distinct values
    for i, name in enumerate(_ALL_STAT_NAMES):
        data[f"p1_{name}"] = [i + 10]
    # P2 stats: use index+100 for distinct values
    for i, name in enumerate(_ALL_STAT_NAMES):
        data[f"p2_{name}"] = [i + 100]
    return pl.DataFrame(data)


class TestExplodeMatchStats:
    def test_produces_two_rows_per_match(self):
        result = explode_match_stats(_make_match_stats_df())
        assert len(result) == 2

    def test_each_player_gets_own_stats(self):
        result = explode_match_stats(_make_match_stats_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        # AAAA was p1 with svc_aces = 10 (index 0 + 10)
        assert row_a["svc_aces"].item() == 10
        # BBBB was p2 with svc_aces = 100 (index 0 + 100)
        assert row_b["svc_aces"].item() == 100

    def test_all_stat_fields_present(self):
        result = explode_match_stats(_make_match_stats_df())
        for name in _ALL_STAT_NAMES:
            assert name in result.columns, f"Missing stat column: {name}"

    def test_no_p1_p2_stat_columns_remain(self):
        result = explode_match_stats(_make_match_stats_df())
        for name in _ALL_STAT_NAMES:
            assert f"p1_{name}" not in result.columns
            assert f"p2_{name}" not in result.columns

    def test_identity_fields_swap(self):
        result = explode_match_stats(_make_match_stats_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["opp_id"].item() == "BBBB"
        assert row_b["opp_id"].item() == "AAAA"
        assert row_a["player_seed"].item() == 1
        assert row_a["opp_seed"].item() is None
        assert row_b["player_seed"].item() is None
        assert row_b["opp_seed"].item() == 1

    def test_won_derived_from_winner_id(self):
        result = explode_match_stats(_make_match_stats_df(winner_id="AAAA"))
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["won"].item() is True
        assert row_b["won"].item() is False

    def test_won_null_when_winner_id_null(self):
        result = explode_match_stats(_make_match_stats_df(winner_id=None))
        assert result["won"].null_count() == 2

    def test_drops_traceability(self):
        result = explode_match_stats(_make_match_stats_df())
        assert "source_file" not in result.columns
        assert "parsed_at" not in result.columns

    def test_drops_winner_id(self):
        result = explode_match_stats(_make_match_stats_df())
        assert "winner_id" not in result.columns

    def test_drops_null_match_uid(self):
        df = _make_match_stats_df(match_uid=None)
        result = explode_match_stats(df)
        assert len(result) == 0

    def test_empty_dataframe(self):
        df = _make_match_stats_df()
        empty = df.clear()
        result = explode_match_stats(empty)
        assert len(result) == 0
        assert "player_id" in result.columns
        assert "svc_aces" in result.columns

    def test_shared_cols_preserved(self):
        result = explode_match_stats(_make_match_stats_df())
        expected_shared = [
            "match_uid", "tournament_id", "year", "circuit",
            "draw_type", "round", "round_id", "match_id",
            "surface", "tournament_start_date", "tournament_end_date",
            "tournament_city", "prize_money", "currency",
            "draw_size_singles", "draw_size_doubles",
            "duration_seconds", "reason", "number_of_sets", "sets_played",
            "is_qualifier", "scoring_system", "court_name",
            "umpire_first_name", "umpire_last_name",
        ]
        for col in expected_shared:
            assert col in result.columns, f"Missing shared column: {col}"

    def test_all_stat_values_correct_for_both_players(self):
        result = explode_match_stats(_make_match_stats_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        for i, name in enumerate(_ALL_STAT_NAMES):
            assert row_a[name].item() == i + 10, f"AAAA {name} wrong"
            assert row_b[name].item() == i + 100, f"BBBB {name} wrong"


# ---------------------------------------------------------------------------
# explode_schedule tests
# ---------------------------------------------------------------------------


def _make_schedule_df(
    *,
    match_uid: str | None = "2023_580_SGL_F_AAAA_BBBB",
) -> pl.DataFrame:
    """Minimal schedule DataFrame with all required columns."""
    return pl.DataFrame(
        {
            "match_uid": [match_uid],
            "tournament_id": ["580"],
            "year": [2023],
            "circuit": ["atp"],
            "draw_type": ["singles"],
            "round": ["Finals"],
            "match_date": [date(2023, 1, 7)],
            "scheduled_datetime": [datetime(2023, 1, 7, 14, 0, 0)],
            "time_suffix": [""],
            "display_time": ["2:00 PM"],
            "court_name": ["Centre Court"],
            "status": ["completed"],
            "score": ["6-4 3-6 7-6(3)"],
            "snapshot_timestamp": [datetime(2023, 1, 7, 10, 0, 0)],
            "p1_id": ["AAAA"],
            "p1_name": ["Player A"],
            "p1_country": ["USA"],
            "p1_seed": [1],
            "p1_entry": [None],
            "p2_id": ["BBBB"],
            "p2_name": ["Player B"],
            "p2_country": ["GBR"],
            "p2_seed": [None],
            "p2_entry": ["Q"],
            "source_file": ["schedule_singles.html"],
            "parsed_at": [datetime(2026, 2, 25, 12, 0, 0)],
        }
    )


class TestExplodeSchedule:
    def test_produces_two_rows_per_match(self):
        result = explode_schedule(_make_schedule_df())
        assert len(result) == 2

    def test_no_won_column(self):
        result = explode_schedule(_make_schedule_df())
        assert "won" not in result.columns

    def test_identity_fields_swap(self):
        result = explode_schedule(_make_schedule_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["opp_id"].item() == "BBBB"
        assert row_b["opp_id"].item() == "AAAA"
        assert row_a["player_seed"].item() == 1
        assert row_a["opp_seed"].item() is None
        assert row_b["player_seed"].item() is None
        assert row_b["opp_seed"].item() == 1

    def test_drops_name_and_country_columns(self):
        result = explode_schedule(_make_schedule_df())
        for col in ["p1_name", "p2_name", "p1_country", "p2_country"]:
            assert col not in result.columns

    def test_drops_traceability_columns(self):
        result = explode_schedule(_make_schedule_df())
        assert "source_file" not in result.columns
        assert "parsed_at" not in result.columns

    def test_drops_snapshot_timestamp(self):
        result = explode_schedule(_make_schedule_df())
        assert "snapshot_timestamp" not in result.columns

    def test_schedule_specific_fields_preserved(self):
        result = explode_schedule(_make_schedule_df())
        expected = [
            "match_uid", "tournament_id", "year", "circuit",
            "draw_type", "round", "match_date", "scheduled_datetime",
            "time_suffix", "display_time", "court_name", "status", "score",
        ]
        for col in expected:
            assert col in result.columns, f"Missing column: {col}"

    def test_drops_null_match_uid(self):
        df = _make_schedule_df(match_uid=None)
        result = explode_schedule(df)
        assert len(result) == 0

    def test_empty_dataframe(self):
        df = _make_schedule_df()
        empty = df.clear()
        result = explode_schedule(empty)
        assert len(result) == 0
        assert "player_id" in result.columns
        assert "opp_id" in result.columns

    def test_entry_field_swap(self):
        result = explode_schedule(_make_schedule_df())
        row_a = result.filter(pl.col("player_id") == "AAAA")
        row_b = result.filter(pl.col("player_id") == "BBBB")
        assert row_a["player_entry"].item() is None
        assert row_a["opp_entry"].item() == "Q"
        assert row_b["player_entry"].item() == "Q"
        assert row_b["opp_entry"].item() is None
