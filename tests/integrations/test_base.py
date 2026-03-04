"""Tests for prediction sync base module."""

from datetime import date, datetime

import polars as pl

from mvp.integrations.base import (
    COLUMN_NAMES,
    COLUMN_SCHEMA,
    FORMULA_COLUMNS,
    PIPELINE_COLUMN_ORDER,
    PIPELINE_COLUMNS,
    USER_COLUMNS,
    prepare_predictions,
)


class TestColumnSchema:
    def test_column_schema_has_34_columns(self):
        assert len(COLUMN_SCHEMA) == 34

    def test_match_uid_is_in_schema(self):
        assert "match_uid" in COLUMN_NAMES

    def test_pipeline_columns_are_subset_of_schema(self):
        assert PIPELINE_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_user_columns_are_subset_of_schema(self):
        assert USER_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_formula_columns_are_subset_of_schema(self):
        assert FORMULA_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_no_column_in_both_pipeline_and_user(self):
        assert PIPELINE_COLUMNS.isdisjoint(USER_COLUMNS)

    def test_column_names_unique(self):
        assert len(COLUMN_NAMES) == len(set(COLUMN_NAMES))


def _make_predictions(**overrides) -> pl.DataFrame:
    """Build a minimal 1-row predictions DataFrame with sensible defaults."""
    defaults = {
        "match_uid": "2024-0001-MS001",
        "p1_id": "PLAYER_A",
        "p2_id": "PLAYER_B",
        "p1_name": "John Smith",
        "p2_name": "Jane Doe",
        "p1_win_prob": 0.65,
        "p2_win_prob": 0.35,
        "p1_elo": 1530.777,
        "p2_elo": 1420.333,
        "tournament_name": "Brisbane",
        "circuit": "tour",
        "surface": "Hard",
        "round": "R32",
        "effective_match_date": date(2024, 1, 15),
        "scheduled_datetime": datetime(2024, 1, 15, 3, 0, 0),
        "model_version": "baseline_v1",
        "predicted_at": datetime(2024, 1, 14, 12, 0, 0),
    }
    defaults.update(overrides)
    return pl.DataFrame([defaults])


class TestPreparePredictions:
    def test_converts_circuit_labels(self):
        df_tour = prepare_predictions(_make_predictions(circuit="tour"))
        assert df_tour["circuit"][0] == "ATP"

        df_chal = prepare_predictions(_make_predictions(circuit="chal"))
        assert df_chal["circuit"][0] == "CH"

    def test_prediction_column_picks_higher_prob(self):
        df = prepare_predictions(_make_predictions(p1_win_prob=0.65, p2_win_prob=0.35))
        assert df["prediction"][0] == "P1"

    def test_prediction_column_picks_p2(self):
        df = prepare_predictions(_make_predictions(p1_win_prob=0.35, p2_win_prob=0.65))
        assert df["prediction"][0] == "P2"

    def test_timezone_conversion_to_ct(self):
        # 3am UTC on Jan 15 = 9pm CT on Jan 14 (UTC-6 in January)
        df = prepare_predictions(
            _make_predictions(scheduled_datetime=datetime(2024, 1, 15, 3, 0, 0))
        )
        assert df["match_date"][0] == "2024-01-14"
        assert df["match_time"][0] == "21:00"

    def test_missing_scheduled_datetime(self):
        df = prepare_predictions(
            _make_predictions(
                scheduled_datetime=None,
                effective_match_date=date(2024, 3, 10),
            )
        )
        assert df["match_time"][0] == ""
        assert df["match_date"][0] == "2024-03-10"

    def test_tournament_day_is_min_date_per_tournament(self):
        row1 = {
            "match_uid": "2024-0001-MS001",
            "p1_id": "A", "p2_id": "B",
            "p1_name": "A Player", "p2_name": "B Player",
            "p1_win_prob": 0.6, "p2_win_prob": 0.4,
            "p1_elo": 1500.0, "p2_elo": 1400.0,
            "tournament_name": "Brisbane",
            "circuit": "tour", "surface": "Hard", "round": "R32",
            "effective_match_date": date(2024, 1, 15),
            "scheduled_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "model_version": "v1",
            "predicted_at": datetime(2024, 1, 14, 12, 0, 0),
        }
        row2 = {
            **row1,
            "match_uid": "2024-0001-MS002",
            "effective_match_date": date(2024, 1, 16),
            "scheduled_datetime": datetime(2024, 1, 16, 10, 0, 0),
        }
        df = prepare_predictions(pl.DataFrame([row1, row2]))
        # Both should get the min date: 2024-01-15 10:00 UTC -> 2024-01-15 04:00 CT
        assert df["tournament_day"][0] == "2024-01-15"
        assert df["tournament_day"][1] == "2024-01-15"

    def test_elo_values_are_rounded(self):
        df = prepare_predictions(
            _make_predictions(p1_elo=1530.777, p2_elo=1420.333)
        )
        assert df["p1_elo"][0] == 1531
        assert df["p2_elo"][0] == 1420

    def test_output_has_correct_columns(self):
        df = prepare_predictions(_make_predictions())
        assert df.columns == PIPELINE_COLUMN_ORDER
        assert set(df.columns) == PIPELINE_COLUMNS
