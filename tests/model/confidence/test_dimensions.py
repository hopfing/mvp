"""Tests for confidence validation dimensions."""

import polars as pl
import pytest

from mvp.model.confidence.dimensions import (
    get_structural_slices,
    get_modifier_slices,
    get_consensus_slices,
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


class TestConsensusSlices:
    def test_returns_consensus_buckets(self, make_oof_df, make_per_model_preds):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=1000)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        per_model = make_per_model_preds(df, n_models=3)
        slices = get_consensus_slices(oof, per_model)
        consensus_keys = [k for k in slices if k.startswith("consensus:")]
        # With 3 models, possible buckets: 3-0, 2-1
        assert len(consensus_keys) >= 1
        assert all("-" in k.split(":")[1] for k in consensus_keys)

    def test_consensus_slices_cover_all_rows(self, make_oof_df, make_per_model_preds):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=500)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        per_model = make_per_model_preds(df, n_models=3)
        slices = get_consensus_slices(oof, per_model)
        consensus_slices = {k: v for k, v in slices.items() if k.startswith("consensus:")}
        total = sum(len(v) for v in consensus_slices.values())
        assert total == 500

    def test_returns_identity_buckets_on_disagreement(self, make_oof_df, make_per_model_preds):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=2000)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        per_model = make_per_model_preds(df, n_models=3, noise_scale=0.3)
        base_names = ["model_a", "model_b", "model_c"]
        slices = get_consensus_slices(oof, per_model, base_names=base_names)
        identity_keys = [k for k in slices if k.startswith("consensus_id:")]
        # With high noise, should have some disagreement with identity labels
        assert len(identity_keys) >= 1

    def test_no_identity_when_all_agree(self, make_oof_df):
        """When all models perfectly agree, no identity slices are produced."""
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=100)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        # All models have identical predictions — always unanimous
        y_prob = df["y_prob"].to_numpy()
        per_model = [y_prob.copy(), y_prob.copy(), y_prob.copy()]
        slices = get_consensus_slices(oof, per_model)
        identity_keys = [k for k in slices if k.startswith("consensus_id:")]
        assert len(identity_keys) == 0

    def test_skipped_for_single_model(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=100)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        slices = get_consensus_slices(oof, [df["y_prob"].to_numpy()])
        assert len(slices) == 0
