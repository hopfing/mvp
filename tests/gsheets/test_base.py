"""Tests for prediction sync base module."""

import json
import logging
from datetime import date, datetime

import polars as pl

from mvp.gsheets.base import (
    COL_LETTERS,
    COLUMN_NAMES,
    COLUMN_SCHEMA,
    FORMULA_COLUMNS,
    PIPELINE_COLUMN_ORDER,
    PIPELINE_COLUMNS,
    USER_COLUMNS,
    generate_formulas,
    merge_predictions,
    prepare_predictions,
)


class TestColumnSchema:
    def test_column_schema_has_45_columns(self):
        assert len(COLUMN_SCHEMA) == 45

    def test_fav_edge_open_precedes_fav_edge(self):
        i = COLUMN_NAMES.index("fav_edge_open")
        assert COLUMN_NAMES[i + 1] == "fav_edge"

    def test_cal_tier_columns_follow_dog_edge(self):
        i = COLUMN_NAMES.index("dog_edge")
        assert COLUMN_NAMES[i + 1] == "cell_cal"
        assert COLUMN_NAMES[i + 2] == "cal_tier"

    def test_cal_columns_have_short_sheet_headers(self):
        from mvp.gsheets.base import SHEET_HEADERS
        assert SHEET_HEADERS[COLUMN_NAMES.index("cell_cal")] == "cal"
        assert SHEET_HEADERS[COLUMN_NAMES.index("cal_tier")] == "tier"

    def test_book2_follows_book(self):
        book_idx = COLUMN_NAMES.index("book")
        assert COLUMN_NAMES[book_idx + 1] == "book2"

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
        "tournament_id": "580",
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

    def test_context_diffs_orient_to_p1_pick(self):
        # p1 is the pick -> age diff reads p1 - p2 unchanged.
        df = prepare_predictions(_make_predictions(
            p1_win_prob=0.65, p2_win_prob=0.35, player_age_diff=-4.2,
        ))
        assert df["prediction"][0] == "P1"
        assert df["pred_prob"][0] == 0.65
        assert df["age_diff"][0] == -4.2

    def test_context_diffs_orient_to_p2_pick(self):
        # p2 is the pick -> age diff flips to picked - opponent.
        df = prepare_predictions(_make_predictions(
            p1_win_prob=0.35, p2_win_prob=0.65, player_age_diff=-4.2,
        ))
        assert df["prediction"][0] == "P2"
        assert df["pred_prob"][0] == 0.65
        assert df["age_diff"][0] == 4.2

    def test_timezone_conversion_to_ct(self):
        # 3am UTC on Jan 15 = 9pm CT on Jan 14 (UTC-6 in January)
        df = prepare_predictions(
            _make_predictions(scheduled_datetime=datetime(2024, 1, 15, 3, 0, 0))
        )
        assert df["date"][0] == "2024-01-14"
        assert df["time"][0] == "21:00"

    def test_missing_scheduled_datetime(self):
        df = prepare_predictions(
            _make_predictions(
                scheduled_datetime=None,
                effective_match_date=date(2024, 3, 10),
            )
        )
        assert df["time"][0] == ""
        assert df["date"][0] == "2024-03-10"

    def test_tournament_day_uses_venue_date(self):
        """tournament_day = venue-local match_date for each ATP session."""
        row1 = {
            "match_uid": "2024-0001-MS001",
            "p1_id": "A", "p2_id": "B",
            "p1_name": "A Player", "p2_name": "B Player",
            "p1_win_prob": 0.6, "p2_win_prob": 0.4,
            "p1_elo": 1500.0, "p2_elo": 1400.0,
            "tournament_id": "580",
            "tournament_name": "Brisbane",
            "circuit": "tour", "surface": "Hard", "round": "R32",
            "effective_match_date": date(2024, 1, 15),
            "match_date": date(2024, 1, 15),
            "scheduled_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "model_version": "v1",
            "predicted_at": datetime(2024, 1, 14, 12, 0, 0),
        }
        row2 = {
            **row1,
            "match_uid": "2024-0001-MS002",
            "effective_match_date": date(2024, 1, 16),
            "match_date": date(2024, 1, 16),
            "scheduled_datetime": datetime(2024, 1, 16, 10, 0, 0),
        }
        df = prepare_predictions(pl.DataFrame([row1, row2]))
        # Different venue dates -> different tournament_days
        assert df["tournament_day"][0] == "2024-01-15"
        assert df["tournament_day"][1] == "2024-01-16"

    def test_tournament_day_cross_midnight_anchors_to_venue_date(self):
        """Asian session that spans CT midnight stays anchored to its venue date.

        Regression test: previously the MIN(CT date) computation pulled these
        matches onto the prior CT day, causing different ATP sessions
        (e.g. Wuning Day 7 R32 vs Day 8 R16) to collide on the same
        tournament_day even though their `schedule_day` values differed.
        """
        base = {
            "p1_id": "A", "p2_id": "B",
            "p1_name": "A Player", "p2_name": "B Player",
            "p1_win_prob": 0.6, "p2_win_prob": 0.4,
            "p1_elo": 1500.0, "p2_elo": 1400.0,
            "tournament_id": "9999",
            "tournament_name": "Yokkaichi",
            "circuit": "chal", "surface": "Hard", "round": "R32",
            "match_date": date(2024, 3, 26),  # venue-local date
            "model_version": "v1",
            "predicted_at": datetime(2024, 3, 25, 12, 0, 0),
        }
        # Early match: 2am UTC Mar 26 = 9pm CT Mar 25
        row1 = {
            **base,
            "match_uid": "MS001",
            "effective_match_date": date(2024, 3, 26),
            "scheduled_datetime": datetime(2024, 3, 26, 2, 0, 0),
        }
        # Late match: 10am UTC Mar 26 = 5am CT Mar 26
        row2 = {
            **base,
            "match_uid": "MS002",
            "effective_match_date": date(2024, 3, 26),
            "scheduled_datetime": datetime(2024, 3, 26, 10, 0, 0),
        }
        df = prepare_predictions(pl.DataFrame([row1, row2]))
        # Per-row CT dates differ across midnight...
        assert df["date"][0] == "2024-03-25"
        assert df["date"][1] == "2024-03-26"
        # ...but tournament_day stays anchored to the venue date for both.
        assert df["tournament_day"][0] == "2024-03-26"
        assert df["tournament_day"][1] == "2024-03-26"

    def test_tournament_day_falls_back_to_ct_date(self):
        df = prepare_predictions(
            _make_predictions(scheduled_datetime=datetime(2024, 1, 15, 10, 0, 0))
        )
        # No match_date -> falls back to CT-converted date
        assert df["tournament_day"][0] == "2024-01-15"

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


def _make_sheet_row(**overrides):
    """Build a single sheet row dict with all columns."""
    defaults = {col: "" for col in COLUMN_NAMES}
    defaults.update({
        "match_uid": "M1",
        "p1_id": "A",
        "p2_id": "B",
        "date": "2024-01-15",
        "time": "09:00",
        "circuit": "ATP",
        "tournament": "Test Open",
        "surface": "Hard",
        "round": "R32",
        "p1": "John Smith",
        "p2": "Jane Doe",
        "p1_elo": "1530",
        "p2_elo": "1470",
        "pred_prob": "0.65",
        "prediction": "P1",
        "tournament_day": "2024-01-15",
        "model_version": "v1",
        "predicted_at": "2024-01-14T12:00:00",
    })
    defaults.update(overrides)
    return defaults


def _sheet_df(rows):
    """Build a DataFrame from list of row dicts, all Utf8."""
    if not rows:
        return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})
    return pl.DataFrame(rows, schema={col: pl.Utf8 for col in COLUMN_NAMES})


def _matches_df(rows):
    """Build a minimal matches DataFrame for result lookup."""
    schema_overrides = {}
    if "won" in rows:
        schema_overrides["won"] = pl.Boolean
    return pl.DataFrame(rows, schema_overrides=schema_overrides)


class TestMergePredictions:
    def test_new_rows_added(self):
        existing = _sheet_df([])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        assert len(result) == 1
        assert result["match_uid"][0] == "2024-0001-MS001"

    def test_existing_rows_user_columns_preserved(self):
        row = _make_sheet_row(
            match_uid="2024-0001-MS001",
            p1_odds="2.10",
            stake="100",
            bet_side="P1",
            notes="my notes",
        )
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        assert len(result) == 1
        assert result["p1_odds"][0] == "2.10"
        assert result["stake"][0] == "100"
        assert result["notes"][0] == "my notes"

    def test_result_auto_filled_when_blank(self):
        row = _make_sheet_row(match_uid="M1", result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M1"],
            "won": [True, False],
            "player_id": ["A", "B"],
            "opp_id": ["B", "A"],
        })
        result = merge_predictions(existing, new, matches)
        m1_row = result.filter(pl.col("match_uid") == "M1")
        assert m1_row["result"][0] == "P1"

    def test_result_p2_wins(self):
        row = _make_sheet_row(match_uid="M1", result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M1"],
            "won": [False, True],
            "player_id": ["A", "B"],
            "opp_id": ["B", "A"],
        })
        result = merge_predictions(existing, new, matches)
        m1_row = result.filter(pl.col("match_uid") == "M1")
        assert m1_row["result"][0] == "P2"

    def test_result_not_overwritten_when_filled(self):
        row = _make_sheet_row(match_uid="M1", result="P2")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M1"],
            "won": [True, False],
            "player_id": ["A", "B"],
            "opp_id": ["B", "A"],
        })
        result = merge_predictions(existing, new, matches)
        m1_row = result.filter(pl.col("match_uid") == "M1")
        assert m1_row["result"][0] == "P2"

    def test_result_mismatch_logs_warning(self, caplog):
        row = _make_sheet_row(match_uid="M1", result="P2")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M1"],
            "won": [True, False],
            "player_id": ["A", "B"],
            "opp_id": ["B", "A"],
        })
        with caplog.at_level(logging.WARNING):
            merge_predictions(existing, new, matches)
        assert "Result mismatch" in caplog.text

    def test_result_uses_sheet_p1_id(self):
        row = _make_sheet_row(match_uid="M1", result="", p1_id="ZVEREV", p2_id="ALCARAZ")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M1"],
            "won": [True, False],
            "player_id": ["ZVEREV", "ALCARAZ"],
            "opp_id": ["ALCARAZ", "ZVEREV"],
        })
        result = merge_predictions(existing, new, matches)
        m1_row = result.filter(pl.col("match_uid") == "M1")
        assert m1_row["result"][0] == "P1"

    def test_sort_order(self):
        row1 = _make_sheet_row(
            match_uid="M1",
            date="2024-01-16",
            tournament_day="2024-01-16",
            tournament="B Open",
            time="10:00",
        )
        row2 = _make_sheet_row(
            match_uid="M2",
            date="2024-01-15",
            tournament_day="2024-01-15",
            tournament="A Open",
            time="14:00",
        )
        existing = _sheet_df([row1, row2])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": ["M1", "M2"],
            "won": [None, None],
            "player_id": ["A", "A"],
            "opp_id": ["B", "B"],
        })
        result = merge_predictions(existing, new, matches)
        uids = result["match_uid"].to_list()
        assert uids.index("M2") < uids.index("M1")

    def test_output_has_all_columns(self):
        existing = _sheet_df([])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        assert list(result.columns) == COLUMN_NAMES
        assert len(result.columns) == 45

    def test_empty_existing_empty_new(self):
        existing = _sheet_df([])
        new = prepare_predictions(
            pl.DataFrame(schema={
                "match_uid": pl.Utf8,
                "p1_id": pl.Utf8,
                "p2_id": pl.Utf8,
                "p1_name": pl.Utf8,
                "p2_name": pl.Utf8,
                "p1_win_prob": pl.Float64,
                "p2_win_prob": pl.Float64,
                "p1_elo": pl.Float64,
                "p2_elo": pl.Float64,
                "tournament_name": pl.Utf8,
                "circuit": pl.Utf8,
                "surface": pl.Utf8,
                "round": pl.Utf8,
                "effective_match_date": pl.Date,
                "scheduled_datetime": pl.Datetime,
                "model_version": pl.Utf8,
                "predicted_at": pl.Datetime,
            })
        )
        matches = _matches_df({
            "match_uid": [],
            "won": [],
            "player_id": [],
            "opp_id": [],
        })
        result = merge_predictions(existing, new, matches)
        assert len(result) == 0
        assert len(result.columns) == 45
        assert list(result.columns) == COLUMN_NAMES

    def test_fav_edge_open_populated_from_opening_odds(self):
        existing = _sheet_df([])
        new = prepare_predictions(_make_predictions(p1_win_prob=0.65, p2_win_prob=0.35))
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["PLAYER_A"],
            "opp_id": ["PLAYER_B"],
        })
        # Opening odds: 1.80 on predicted side P1 -> implied 0.5556 -> edge 0.0944
        opening = {
            "BookA": {"2024-0001-MS001": {"PLAYER_A": 1.80, "PLAYER_B": 2.00}},
            "BookB": {"2024-0001-MS001": {"PLAYER_A": 1.70, "PLAYER_B": 2.10}},
        }
        result = merge_predictions(
            existing, new, matches, opening_odds_maps=opening,
        )
        assert len(result) == 1
        # Best opening on P1 side is 1.80 (max across books) -> edge = 0.65 - 1/1.80 = 0.0944
        edge_str = result["fav_edge_open"][0]
        assert edge_str != ""
        assert abs(float(edge_str) - (0.65 - 1.0 / 1.80)) < 1e-3

    def test_fav_edge_open_not_overwritten_once_set(self):
        row = _make_sheet_row(
            match_uid="2024-0001-MS001",
            fav_edge_open="0.1234",
        )
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["PLAYER_A"],
            "opp_id": ["PLAYER_B"],
        })
        opening = {
            "BookA": {"2024-0001-MS001": {"PLAYER_A": 1.50, "PLAYER_B": 2.50}},
        }
        result = merge_predictions(
            existing, new, matches, opening_odds_maps=opening,
        )
        # Existing value preserved, not recomputed from current opening odds
        assert result["fav_edge_open"][0] == "0.1234"

    def test_cal_tier_populated_from_sidecar(self, tmp_path, monkeypatch):
        # Sidecar keys on raw circuit values ("tour"/"chal") because that's
        # what diagnostics emits. The sheet rows store display labels
        # ("ATP"/"CH") via prepare_predictions. The lookup must translate
        # before matching — this test uses the display labels to lock that in.
        sidecar = tmp_path / "lead_cal_tiers.json"
        sidecar.write_text(json.dumps({
            "segments": {
                "by_circuit": {
                    "tour": {"round": {"R32": {"signed_calibration": -0.0073}}},
                    "chal": {"round": {"R16": {"signed_calibration": 0.0010}}},
                },
            },
        }))
        monkeypatch.setattr(
            "mvp.gsheets.base._resolve_lead_sidecar_path", lambda: sidecar
        )

        existing = _sheet_df([])
        new = pl.DataFrame([{c: "" for c in COLUMN_NAMES}]).with_columns(
            pl.lit("M_NEW").alias("match_uid"),
            pl.lit("ATP").alias("circuit"),  # display label, not raw "tour"
            pl.lit("R32").alias("round"),
        )
        matches = _matches_df({
            "match_uid": ["M_NEW"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        m = result.filter(pl.col("match_uid") == "M_NEW")
        assert m["cal_tier"][0] == "Risky"  # -0.0073 is in [-0.01, -0.005)
        assert abs(float(m["cell_cal"][0]) - (-0.0073)) < 1e-6

    def test_cal_tier_not_overwritten_once_set(self, tmp_path, monkeypatch):
        # Sidecar would suggest "Optimal" for this (circuit, round), but the
        # row already has "Risky" — frozen-once-set must preserve "Risky".
        sidecar = tmp_path / "lead_cal_tiers.json"
        sidecar.write_text(json.dumps({
            "segments": {
                "by_circuit": {
                    "tour": {"round": {"R32": {"signed_calibration": 0.005}}},
                },
            },
        }))
        monkeypatch.setattr(
            "mvp.gsheets.base._resolve_lead_sidecar_path", lambda: sidecar
        )

        row = _make_sheet_row(
            match_uid="M1",
            circuit="ATP",  # sheet display label
            round="R32",
            cal_tier="Risky",
            cell_cal="-0.0080",
        )
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": [],
            "won": [],
            "player_id": [],
            "opp_id": [],
        })
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["cal_tier"][0] == "Risky"
        assert m1["cell_cal"][0] == "-0.0080"

    def test_cal_tier_not_backfilled_on_preexisting_blank_rows(self, tmp_path, monkeypatch):
        # Regression: pre-existing rows with blank cal_tier must NOT get filled
        # from the current sidecar — that would stamp historical bets (placed
        # under earlier model state) with today's tier values, poisoning the
        # historical analysis. Only matches first appearing in this sync should
        # get populated from the current sidecar.
        sidecar = tmp_path / "lead_cal_tiers.json"
        sidecar.write_text(json.dumps({
            "segments": {
                "by_circuit": {
                    "tour": {"round": {"R32": {"signed_calibration": -0.0073}}},
                },
            },
        }))
        monkeypatch.setattr(
            "mvp.gsheets.base._resolve_lead_sidecar_path", lambda: sidecar
        )

        # Pre-existing row from before the cal_tier feature shipped: blank
        # cal_tier and cell_cal, but the match would map to tour/R32 which IS
        # in the sidecar. The fix must leave it alone.
        old_row = _make_sheet_row(
            match_uid="OLD_MATCH",
            circuit="ATP",
            round="R32",
            cal_tier="",
            cell_cal="",
        )
        existing = _sheet_df([old_row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({
            "match_uid": [],
            "won": [],
            "player_id": [],
            "opp_id": [],
        })
        result = merge_predictions(existing, new, matches)
        old = result.filter(pl.col("match_uid") == "OLD_MATCH")
        assert old["cal_tier"][0] == "", (
            f"pre-existing blank row was backfilled to {old['cal_tier'][0]!r}"
        )
        assert old["cell_cal"][0] == ""

    def test_cal_tier_blank_when_no_sidecar(self, monkeypatch):
        monkeypatch.setattr(
            "mvp.gsheets.base._resolve_lead_sidecar_path", lambda: None
        )

        existing = _sheet_df([])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["PLAYER_A"],
            "opp_id": ["PLAYER_B"],
        })
        result = merge_predictions(existing, new, matches)
        # No sidecar -> empty lookup -> blank tier and cell_cal
        assert result["cal_tier"][0] == ""
        assert result["cell_cal"][0] == ""

    def test_duplicate_match_uid_not_added(self):
        row = _make_sheet_row(match_uid="2024-0001-MS001")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions())
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        assert len(result) == 1


    def test_bet_result_derived_win(self):
        row = _make_sheet_row(match_uid="M1", result="P1", bet_side="P1", bet_result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["bet_result"][0] == "W"

    def test_bet_result_derived_loss(self):
        row = _make_sheet_row(match_uid="M1", result="P2", bet_side="P1", bet_result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["bet_result"][0] == "L"

    def test_bet_result_not_overwritten(self):
        row = _make_sheet_row(match_uid="M1", result="P1", bet_side="P2", bet_result="V")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["bet_result"][0] == "V"

    def test_bet_result_skipped_when_no_bet_side(self):
        row = _make_sheet_row(match_uid="M1", result="P1", bet_side="", bet_result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["bet_result"][0] == ""

    def test_bet_result_skipped_when_no_result(self):
        row = _make_sheet_row(match_uid="M1", result="", bet_side="P1", bet_result="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["bet_result"][0] == ""


    def test_odds_auto_filled_when_no_stake(self):
        row = _make_sheet_row(match_uid="M1", p1_odds="", p2_odds="", stake="", book="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {
            "DraftKings": {"M1": {"A": 2.10, "B": 1.75}},
            "BetRivers": {"M1": {"A": 2.05, "B": 1.80}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["p1_odds"][0] == "2.10"  # best for p1 is DK
        assert m1["p2_odds"][0] == "1.80"  # best for p2 is BR

    def test_odds_not_overwritten_when_stake_filled(self):
        row = _make_sheet_row(
            match_uid="M1", p1_odds="2.00", p2_odds="1.70",
            stake="10", book="Bet365",
        )
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {"DraftKings": {"M1": {"A": 2.50, "B": 1.50}}}
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["p1_odds"][0] == "2.00"
        assert m1["p2_odds"][0] == "1.70"
        assert m1["book"][0] == "Bet365"

    def test_book_filled_with_best_for_predicted_side(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {
            "DraftKings": {"M1": {"A": 2.00, "B": 1.80}},
            "Rivers": {"M1": {"A": 2.15, "B": 1.72}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "Rivers"  # Rivers has better p1 odds

    def test_odds_updated_on_subsequent_runs(self):
        """Odds should update each run until a stake is placed."""
        row = _make_sheet_row(match_uid="M1", p1_odds="1.90", p2_odds="1.85", stake="", book="DK")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {"DraftKings": {"M1": {"A": 2.10, "B": 1.75}}}
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["p1_odds"][0] == "2.10"
        assert m1["p2_odds"][0] == "1.75"

    def test_schedule_columns_refreshed_on_existing_rows(self):
        row = _make_sheet_row(
            match_uid="2024-0001-MS001",
            date="2024-01-14",
            time="21:00",
            round="R32",
            tournament="Brisbane",
            surface="Hard",
            circuit="ATP",
            tournament_day="2024-01-14",
            pred_prob="0.65",
        )
        existing = _sheet_df([row])
        # New prediction has updated schedule (match moved to next day)
        new = prepare_predictions(_make_predictions(
            scheduled_datetime=datetime(2024, 1, 16, 3, 0, 0),
            effective_match_date=date(2024, 1, 16),
        ))
        matches = _matches_df({
            "match_uid": ["2024-0001-MS001"],
            "won": [None],
            "player_id": ["A"],
            "opp_id": ["B"],
        })
        result = merge_predictions(existing, new, matches)
        assert len(result) == 1
        # Schedule columns should be updated
        assert result["date"][0] == "2024-01-15"  # CT conversion of Jan 16 3am UTC
        assert result["time"][0] == "21:00"
        assert result["tournament_day"][0] == "2024-01-15"
        # Prediction columns should NOT be updated
        assert result["pred_prob"][0] == "0.65"

    def test_book2_filled_when_two_books_tie_at_best(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        # BR and DK tie at 2.10 for p1; MGM is 2.05
        odds_maps = {
            "BR": {"M1": {"A": 2.10, "B": 1.80}},
            "DK": {"M1": {"A": 2.10, "B": 1.75}},
            "MGM": {"M1": {"A": 2.05, "B": 1.82}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "BR"    # first in registry order
        assert m1["book2"][0] == "DK"   # second tied

    def test_book2_blank_when_no_tie(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {
            "DK": {"M1": {"A": 2.15, "B": 1.75}},
            "BR": {"M1": {"A": 2.10, "B": 1.80}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "DK"
        assert m1["book2"][0] == ""

    def test_tie_break_rounds_to_two_decimals(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        # 2.104 and 2.102 both round to 2.10 -> tie
        odds_maps = {
            "BR": {"M1": {"A": 2.104, "B": 1.80}},
            "DK": {"M1": {"A": 2.102, "B": 1.75}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "BR"
        assert m1["book2"][0] == "DK"

    def test_book2_filled_when_within_penny_tolerance(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        # B365 has strict best at 1.78; BR and DK at 1.77 are within 0.01
        odds_maps = {
            "B365": {"M1": {"A": 1.78, "B": 2.00}},
            "BR": {"M1": {"A": 1.77, "B": 2.05}},
            "DK": {"M1": {"A": 1.77, "B": 2.05}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "B365"
        assert m1["book2"][0] == "BR"

    def test_book2_excluded_when_gap_exceeds_penny(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {
            "B365": {"M1": {"A": 1.80, "B": 2.00}},
            "BR": {"M1": {"A": 1.78, "B": 2.05}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "B365"
        assert m1["book2"][0] == ""

    def test_three_way_tie_takes_first_two(self):
        row = _make_sheet_row(match_uid="M1", prediction="P1", stake="", book="", book2="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        odds_maps = {
            "BR": {"M1": {"A": 2.10, "B": 1.80}},
            "DK": {"M1": {"A": 2.10, "B": 1.75}},
            "MGM": {"M1": {"A": 2.10, "B": 1.82}},
        }
        result = merge_predictions(existing, new, matches, odds_maps=odds_maps)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["book"][0] == "BR"
        assert m1["book2"][0] == "DK"

    def test_no_odds_maps_leaves_odds_unchanged(self):
        row = _make_sheet_row(match_uid="M1", p1_odds="2.00", stake="")
        existing = _sheet_df([row])
        new = prepare_predictions(_make_predictions(match_uid="OTHER"))
        matches = _matches_df({"match_uid": [], "won": [], "player_id": [], "opp_id": []})
        result = merge_predictions(existing, new, matches)
        m1 = result.filter(pl.col("match_uid") == "M1")
        assert m1["p1_odds"][0] == "2.00"


class TestColLetters:
    def test_first_column_is_A(self):
        assert COL_LETTERS[COLUMN_NAMES[0]] == "A"

    def test_26th_column_is_Z(self):
        assert COL_LETTERS[COLUMN_NAMES[25]] == "Z"

    def test_27th_column_is_AA(self):
        assert COL_LETTERS[COLUMN_NAMES[26]] == "AA"


class TestGenerateFormulas:
    def test_returns_all_formula_columns(self):
        formulas = generate_formulas(row=2)
        assert set(formulas.keys()) == FORMULA_COLUMNS

    def test_fav_edge_formula(self):
        formulas = generate_formulas(row=2)
        assert formulas["fav_edge"].startswith("=IF(")
        assert "1/" in formulas["fav_edge"]

    def test_dog_edge_formula(self):
        formulas = generate_formulas(row=2)
        assert formulas["dog_edge"].startswith("=IF(")
        assert "1/" in formulas["dog_edge"]

    def test_to_win_is_stake_times_odds(self):
        formulas = generate_formulas(row=2)
        assert COL_LETTERS["stake"] in formulas["to_win"]
        assert COL_LETTERS["bet_odds"] in formulas["to_win"]

    def test_net_references_bet_result(self):
        formulas = generate_formulas(row=2)
        assert '"W"' in formulas["net"]
        assert '"L"' in formulas["net"]
        assert '"V"' in formulas["net"]

    def test_formulas_use_correct_row_number(self):
        f2 = generate_formulas(row=2)
        f5 = generate_formulas(row=5)
        assert "2" in f2["fav_edge"] and "5" not in f2["fav_edge"]
        assert "5" in f5["fav_edge"] and "2" not in f5["fav_edge"]

    def test_all_formulas_start_with_equals(self):
        formulas = generate_formulas(row=2)
        for name, formula in formulas.items():
            assert formula.startswith("="), f"{name} doesn't start with ="
