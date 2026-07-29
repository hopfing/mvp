"""Tests for the prediction alert-rule engine."""

from datetime import datetime

import polars as pl
import pytest

from mvp import alerts

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=alerts.CT)


def write_rules(tmp_path, text):
    path = tmp_path / "alerts.yaml"
    path.write_text(text)
    return path


def bet_row(**overrides):
    """A sheet row that is open, has odds, and matches nothing in particular."""
    row = {
        "date": "2026-07-29",
        "time": "18:00",
        "circuit": "CH",
        "tournament": "Segovia",
        "surface": "Clay",
        "round": "QF",
        "prediction": "P1",
        "pred_prob": "0.62",
        "consensus": "1.0",
        "fav_edge_open": "0.0400",
        "cell_cal": "0.01",
        "cal_tier": "optimal",
        "age_diff": "-3.5",
        "court": "outdoor",
        "book": "betmgm",
        "p1_odds": "1.90",
        "p2_odds": "2.00",
        "stake": "",
        "match_uid": "m1",
    }
    row.update(overrides)
    return row


def frame(*rows):
    return pl.DataFrame(list(rows))


# --- load_rules ---------------------------------------------------------


def test_missing_file_means_alerting_off(tmp_path):
    assert alerts.load_rules(tmp_path / "nope.yaml") == []


def test_empty_rules_list_parses(tmp_path):
    assert alerts.load_rules(write_rules(tmp_path, "rules: []")) == []


def test_parses_conditions(tmp_path):
    path = write_rules(tmp_path, """
rules:
  - name: Clay challenger value
    match:
      edge: 0.05
      consensus: 1.0
      circuit: chal
      surface: [clay]
""")
    rules = alerts.load_rules(path)
    assert len(rules) == 1
    assert rules[0].name == "Clay challenger value"
    assert rules[0].conditions == {
        "edge": 0.05, "consensus": 1.0, "circuit": "chal", "surface": ["clay"],
    }


def test_unknown_field_rejected(tmp_path):
    path = write_rules(tmp_path, """
rules:
  - name: Typo
    match:
      surfce: [clay]
""")
    with pytest.raises(ValueError, match="unknown field 'surfce'"):
        alerts.load_rules(path)


def test_duplicate_rule_name_rejected(tmp_path):
    path = write_rules(tmp_path, """
rules:
  - name: Same
    match: {edge: 0.05}
  - name: Same
    match: {edge: 0.08}
""")
    with pytest.raises(ValueError, match="duplicate rule name"):
        alerts.load_rules(path)


def test_rule_without_name_rejected(tmp_path):
    path = write_rules(tmp_path, "rules:\n  - match: {edge: 0.05}\n")
    with pytest.raises(ValueError, match="needs a non-empty 'name'"):
        alerts.load_rules(path)


def test_rule_without_conditions_rejected(tmp_path):
    path = write_rules(tmp_path, "rules:\n  - name: Empty\n    match: {}\n")
    with pytest.raises(ValueError, match="needs a non-empty 'match'"):
        alerts.load_rules(path)


def test_string_spec_on_numeric_field_rejected(tmp_path):
    path = write_rules(tmp_path, 'rules:\n  - name: R\n    match: {edge: ">=0.05"}\n')
    with pytest.raises(ValueError, match="takes a number"):
        alerts.load_rules(path)


def test_unknown_bound_key_rejected(tmp_path):
    path = write_rules(tmp_path, "rules:\n  - name: R\n    match:\n      edge: {mn: 0.05}\n")
    with pytest.raises(ValueError, match="'min' and/or 'max'"):
        alerts.load_rules(path)


def test_number_on_string_field_rejected(tmp_path):
    path = write_rules(tmp_path, "rules:\n  - name: R\n    match: {circuit: 1}\n")
    with pytest.raises(ValueError, match="takes a string"):
        alerts.load_rules(path)


def test_min_max_bounds_parse(tmp_path):
    path = write_rules(tmp_path, """
rules:
  - name: Banded
    match:
      pred_prob: {min: 0.55, max: 0.80}
""")
    assert alerts.load_rules(path)[0].conditions["pred_prob"] == {
        "min": 0.55, "max": 0.80,
    }


# --- row_fields ---------------------------------------------------------


def test_edge_uses_p1_odds_for_a_p1_pick():
    fields = alerts.row_fields(bet_row(prediction="P1", pred_prob="0.62", p1_odds="2.00"))
    assert fields["edge"] == pytest.approx(0.12)
    assert fields["pred_odds"] == 2.00


def test_edge_uses_p2_odds_for_a_p2_pick():
    fields = alerts.row_fields(bet_row(prediction="P2", pred_prob="0.62", p2_odds="2.50"))
    assert fields["edge"] == pytest.approx(0.22)


def test_edge_is_none_without_odds():
    fields = alerts.row_fields(bet_row(p1_odds=""))
    assert fields["edge"] is None
    assert fields["pred_odds"] is None


def test_edge_is_none_when_prob_unparseable():
    assert alerts.row_fields(bet_row(pred_prob=""))["edge"] is None


def test_circuit_normalized_to_raw_vocabulary():
    assert alerts.row_fields(bet_row(circuit="CH"))["circuit"] == "chal"
    assert alerts.row_fields(bet_row(circuit="ATP"))["circuit"] == "tour"


def test_open_edge_read_from_frozen_column():
    assert alerts.row_fields(bet_row(fav_edge_open="0.0400"))["edge_open"] == 0.04


def test_blank_strings_become_none():
    fields = alerts.row_fields(bet_row(cal_tier="", court="", consensus=""))
    assert fields["cal_tier"] is None
    assert fields["court"] is None
    assert fields["consensus"] is None


# --- matching -----------------------------------------------------------


def match(conditions, **overrides):
    rule = alerts.Rule(name="R", conditions=conditions)
    return alerts.rule_matches(rule, alerts.row_fields(bet_row(**overrides)))


def test_bare_number_is_an_inclusive_minimum():
    assert match({"pred_prob": 0.62}, pred_prob="0.62")
    assert match({"pred_prob": 0.60}, pred_prob="0.62")
    assert not match({"pred_prob": 0.63}, pred_prob="0.62")


def test_consensus_minimum_admits_only_unanimous_at_one():
    assert match({"consensus": 1.0}, consensus="1.0")
    assert not match({"consensus": 1.0}, consensus="0.5")


def test_bounds_are_inclusive():
    assert match({"pred_prob": {"min": 0.55, "max": 0.80}}, pred_prob="0.55")
    assert match({"pred_prob": {"min": 0.55, "max": 0.80}}, pred_prob="0.80")
    assert not match({"pred_prob": {"min": 0.55, "max": 0.80}}, pred_prob="0.81")


def test_max_only_bound():
    assert match({"pred_odds": {"max": 3.0}}, p1_odds="2.50")
    assert not match({"pred_odds": {"max": 3.0}}, p1_odds="3.50")


def test_negative_bound_on_signed_field():
    assert match({"age_diff": {"max": -2.0}}, age_diff="-3.5")
    assert not match({"age_diff": {"max": -2.0}}, age_diff="1.0")


def test_string_comparison_is_case_insensitive():
    assert match({"surface": "clay"}, surface="Clay")
    assert match({"court": "INDOOR"}, court="indoor")


def test_list_is_membership():
    assert match({"surface": ["clay", "grass"]}, surface="Grass")
    assert not match({"surface": ["clay", "grass"]}, surface="Hard")


def test_all_conditions_must_hold():
    conditions = {"edge": 0.05, "circuit": "chal", "surface": ["clay"]}
    assert match(conditions, prediction="P1", pred_prob="0.62", p1_odds="2.00")
    assert not match(
        conditions, prediction="P1", pred_prob="0.62", p1_odds="2.00", circuit="ATP"
    )


def test_missing_value_never_matches():
    assert not match({"edge": 0.05}, p1_odds="")
    assert not match({"cal_tier": "optimal"}, cal_tier="")
    # A minimum of zero must not be satisfied by an absent value.
    assert not match({"edge": 0.0}, p1_odds="")


# --- eligibility --------------------------------------------------------


def test_placed_bet_is_not_open():
    assert not alerts.is_open(bet_row(stake="25"), NOW)


def test_started_match_is_not_open():
    assert not alerts.is_open(bet_row(date="2026-07-29", time="11:00"), NOW)


def test_upcoming_match_is_open():
    assert alerts.is_open(bet_row(date="2026-07-29", time="18:00"), NOW)


def test_unscheduled_match_stays_open_on_its_own_day():
    assert alerts.is_open(bet_row(date="2026-07-29", time=""), NOW)
    assert not alerts.is_open(bet_row(date="2026-07-28", time=""), NOW)


def test_row_without_a_date_is_not_open():
    assert not alerts.is_open(bet_row(date=""), NOW)


# --- ledger -------------------------------------------------------------


def test_ledger_round_trip(tmp_path):
    path = tmp_path / "alerts.jsonl"
    alerts.append_ledger(path, [("Rule A", "m1"), ("Rule B", "m2")], NOW)
    assert alerts.load_ledger(path) == {("Rule A", "m1"), ("Rule B", "m2")}


def test_missing_ledger_is_empty(tmp_path):
    assert alerts.load_ledger(tmp_path / "nope.jsonl") == set()


def test_malformed_ledger_line_skipped(tmp_path):
    path = tmp_path / "alerts.jsonl"
    path.write_text('{"rule": "A", "match_uid": "m1"}\nnot json\n\n')
    assert alerts.load_ledger(path) == {("A", "m1")}


# --- run ----------------------------------------------------------------


RULES = """
rules:
  - name: Clay challenger value
    match:
      edge: 0.05
      consensus: 1.0
      circuit: chal
      surface: [clay]
"""


def test_run_counts_matching_predictions(tmp_path):
    merged = frame(
        bet_row(match_uid="m1", pred_prob="0.62", p1_odds="2.00"),
        bet_row(match_uid="m2", pred_prob="0.62", p1_odds="2.00"),
        bet_row(match_uid="m3", pred_prob="0.52", p1_odds="1.90"),  # edge too thin
    )
    counts = alerts.run(
        merged,
        ledger_path=tmp_path / "alerts.jsonl",
        rules_path=write_rules(tmp_path, RULES),
        now=NOW,
    )
    assert counts == {"Clay challenger value": 2}


def test_run_does_not_refire_the_same_match(tmp_path):
    merged = frame(bet_row(match_uid="m1", pred_prob="0.62", p1_odds="2.00"))
    kwargs = {
        "ledger_path": tmp_path / "alerts.jsonl",
        "rules_path": write_rules(tmp_path, RULES),
        "now": NOW,
    }
    assert alerts.run(merged, **kwargs) == {"Clay challenger value": 1}
    assert alerts.run(merged, **kwargs) == {}


def test_run_fires_when_price_drifts_into_range(tmp_path):
    kwargs = {
        "ledger_path": tmp_path / "alerts.jsonl",
        "rules_path": write_rules(tmp_path, RULES),
        "now": NOW,
    }
    thin = frame(bet_row(match_uid="m1", pred_prob="0.62", p1_odds="1.70"))  # edge 0.032
    assert alerts.run(thin, **kwargs) == {}
    drifted = frame(bet_row(match_uid="m1", pred_prob="0.62", p1_odds="2.00"))
    assert alerts.run(drifted, **kwargs) == {"Clay challenger value": 1}


def test_run_skips_placed_and_started_rows(tmp_path):
    merged = frame(
        bet_row(match_uid="m1", pred_prob="0.62", p1_odds="2.00", stake="25"),
        bet_row(match_uid="m2", pred_prob="0.62", p1_odds="2.00", time="11:00"),
    )
    counts = alerts.run(
        merged,
        ledger_path=tmp_path / "alerts.jsonl",
        rules_path=write_rules(tmp_path, RULES),
        now=NOW,
    )
    assert counts == {}


def test_run_without_rules_writes_no_ledger(tmp_path):
    ledger = tmp_path / "alerts.jsonl"
    merged = frame(bet_row(pred_prob="0.62", p1_odds="2.00"))
    assert alerts.run(
        merged, ledger_path=ledger, rules_path=write_rules(tmp_path, "rules: []"), now=NOW
    ) == {}
    assert not ledger.exists()


def test_run_on_empty_frame(tmp_path):
    assert alerts.run(
        pl.DataFrame(),
        ledger_path=tmp_path / "alerts.jsonl",
        rules_path=write_rules(tmp_path, RULES),
        now=NOW,
    ) == {}


def test_independent_rules_each_fire(tmp_path):
    path = write_rules(tmp_path, """
rules:
  - name: Clay value
    match:
      edge: 0.05
      surface: [clay]
  - name: Short prices
    match:
      pred_odds: {max: 2.50}
""")
    merged = frame(bet_row(match_uid="m1", pred_prob="0.62", p1_odds="2.00"))
    counts = alerts.run(merged, ledger_path=tmp_path / "a.jsonl", rules_path=path, now=NOW)
    assert counts == {"Clay value": 1, "Short prices": 1}
