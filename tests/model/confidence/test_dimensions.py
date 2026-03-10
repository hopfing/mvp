"""Tests for confidence validation dimensions."""

import polars as pl
import pytest

from mvp.model.confidence.dimensions import (
    get_structural_slices,
    get_modifier_slices,
    MODIFIERS,
)


class TestStructuralSlices:
    def test_circuit_slices(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=200)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_structural_slices(oof)
        circuit_slices = {k: v for k, v in slices.items() if k.startswith("circuit:")}
        assert "circuit:chal" in circuit_slices
        assert "circuit:tour" in circuit_slices
        assert len(circuit_slices["circuit:chal"]) + len(circuit_slices["circuit:tour"]) == 200

    def test_surface_slices(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=300, surfaces=["hard", "clay", "grass"])
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_structural_slices(oof)
        surface_slices = {k: v for k, v in slices.items() if k.startswith("surface:")}
        assert len(surface_slices) == 3

    def test_round_slices(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=300, rounds=["Q1", "R32", "QF", "F"])
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_structural_slices(oof)
        round_slices = {k: v for k, v in slices.items() if k.startswith("round:")}
        assert len(round_slices) == 4

    def test_intersection_slices(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=600, circuits=["chal", "tour"], surfaces=["hard", "clay"])
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_structural_slices(oof)
        intersection_slices = {k: v for k, v in slices.items() if "+" in k}
        assert len(intersection_slices) >= 4


class TestModifierSlices:
    def test_elo_level_quintiles(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=500)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_modifier_slices(oof)
        elo_slices = {k: v for k, v in slices.items() if k.startswith("elo_level:")}
        assert len(elo_slices) == 5

    def test_skips_missing_columns(self):
        df = pl.DataFrame({
            "match_uid": ["a"],
            "effective_match_date": [None],
            "favored_prob": [0.6],
            "favored_won": [1],
            "prob_bucket": ["55-60%"],
            "circuit": ["chal"],
            "surface": ["hard"],
            "round": ["R32"],
        })
        slices = get_modifier_slices(df)
        elo_slices = {k: v for k, v in slices.items() if k.startswith("elo_level:")}
        assert len(elo_slices) == 0
