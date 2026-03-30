"""Structural dimensions and hypothesized modifiers for confidence validation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass
class Modifier:
    name: str
    description: str
    required_columns: list[str]
    compute_value: Callable[[pl.DataFrame], pl.Series]
    n_buckets: int = 5  # quintiles by default


MODIFIERS: list[Modifier] = [
    Modifier(
        name="elo_level",
        description="Average base Elo of both players",
        required_columns=["player_elo", "opp_elo"],
        compute_value=lambda df: (df["player_elo"] + df["opp_elo"]) / 2,
    ),
    Modifier(
        name="max_elo_rd",
        description="Maximum Elo RD (higher = more uncertainty)",
        required_columns=["player_elo_rd", "opp_elo_rd"],
        compute_value=lambda df: df.select(
            pl.max_horizontal("player_elo_rd", "opp_elo_rd").alias("v")
        )["v"],
    ),
    Modifier(
        name="rank_gap",
        description="Absolute ranking difference",
        required_columns=["player_rank", "opp_rank"],
        compute_value=lambda df: (df["player_rank"] - df["opp_rank"]).abs().cast(pl.Float64),
    ),
    Modifier(
        name="rank_elo_divergence",
        description="Disagreement between ranking gap and Elo gap direction",
        required_columns=["player_rank", "opp_rank", "player_elo", "opp_elo"],
        compute_value=lambda df: _rank_elo_divergence(df),
    ),
    Modifier(
        name="recent_match_count",
        description="Min recent match count (either player, 30d window)",
        required_columns=["player_match_count_30d", "opp_match_count_30d"],
        compute_value=lambda df: df.select(
            pl.min_horizontal("player_match_count_30d", "opp_match_count_30d").alias("v")
        )["v"],
    ),
    Modifier(
        name="surface_match_count",
        description="Min surface match count (either player)",
        required_columns=["player_surface_matches", "opp_surface_matches"],
        compute_value=lambda df: df.select(
            pl.min_horizontal("player_surface_matches", "opp_surface_matches").alias("v")
        )["v"],
    ),
    Modifier(
        name="tour_match_pct",
        description="Min tour match percentage (either player)",
        required_columns=["player_tour_match_pct", "opp_tour_match_pct"],
        compute_value=lambda df: df.select(
            pl.min_horizontal("player_tour_match_pct", "opp_tour_match_pct").alias("v")
        )["v"],
    ),
    Modifier(
        name="h2h_depth",
        description="Head-to-head match history depth",
        required_columns=["player_h2h_surface_win_pct_365d"],
        compute_value=lambda df: df["player_h2h_surface_win_pct_365d"].is_not_null().cast(pl.Float64),
        n_buckets=0,  # binary: has h2h history or not
    ),
]


def _rank_elo_divergence(df: pl.DataFrame) -> pl.Series:
    """Compute rank-vs-Elo divergence."""
    rank_diff = df["opp_rank"].cast(pl.Float64) - df["player_rank"].cast(pl.Float64)
    elo_diff = df["player_elo"] - df["opp_elo"]
    rank_mean = rank_diff.abs().mean()
    elo_mean = elo_diff.abs().mean()
    if rank_mean == 0 or rank_mean is None or elo_mean == 0 or elo_mean is None:
        return pl.Series("divergence", [0.0] * len(df))
    rank_norm = rank_diff / rank_mean
    elo_norm = elo_diff / elo_mean
    return rank_norm * elo_norm


def get_structural_slices(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Slice OOF data by structural dimensions."""
    slices: dict[str, pl.DataFrame] = {}

    for dim in ["circuit", "surface", "round"]:
        for val in df[dim].unique().sort().to_list():
            if val is None:
                continue
            slices[f"{dim}:{val}"] = df.filter(pl.col(dim) == val)

    # Circuit x Surface intersections
    for circuit in df["circuit"].unique().sort().to_list():
        if circuit is None:
            continue
        circuit_df = df.filter(pl.col("circuit") == circuit)
        for surface in circuit_df["surface"].unique().sort().to_list():
            if surface is None:
                continue
            slices[f"circuit+surface:{circuit}+{surface}"] = circuit_df.filter(pl.col("surface") == surface)

    # Circuit x Round intersections
    for circuit in df["circuit"].unique().sort().to_list():
        if circuit is None:
            continue
        circuit_df = df.filter(pl.col("circuit") == circuit)
        for rnd in circuit_df["round"].unique().sort().to_list():
            if rnd is None:
                continue
            slices[f"circuit+round:{circuit}+{rnd}"] = circuit_df.filter(pl.col("round") == rnd)

    return slices


def get_modifier_slices(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Slice OOF data by hypothesized modifiers. Skips if required columns missing."""
    slices: dict[str, pl.DataFrame] = {}

    for mod in MODIFIERS:
        if not all(c in df.columns for c in mod.required_columns):
            continue

        try:
            values = mod.compute_value(df)
        except Exception:
            continue

        if mod.n_buckets == 0:
            for val in values.unique().sort().to_list():
                if val is None:
                    continue
                label = f"{mod.name}:{int(val)}"
                mask = values == val
                slices[label] = df.filter(mask)
        else:
            try:
                quantiles = [
                    values.quantile(q, interpolation="linear")
                    for q in np.linspace(0, 1, mod.n_buckets + 1)
                ]
            except Exception:
                continue

            for i in range(mod.n_buckets):
                lo = quantiles[i]
                hi = quantiles[i + 1]
                if lo is None or hi is None:
                    continue
                label = f"{mod.name}:Q{i + 1}({lo:.0f}-{hi:.0f})"
                if i < mod.n_buckets - 1:
                    mask = (values >= lo) & (values < hi)
                else:
                    mask = (values >= lo) & (values <= hi)
                filtered = df.filter(mask)
                if len(filtered) > 0:
                    slices[label] = filtered

    return slices


def get_consensus_slices(
    df: pl.DataFrame,
    per_model_preds: list[np.ndarray],
    base_names: list[str] | None = None,
) -> dict[str, pl.DataFrame]:
    """Slice OOF data by ensemble consensus strength and identity.

    Produces two types of slices:
    - consensus:N-M — N models agree with ensemble, M disagree
    - consensus_id:X+Y — which specific models agree (only for non-unanimous)

    Args:
        df: OOF DataFrame (must have y_prob column).
        per_model_preds: Per-base-model prediction arrays, aligned with df rows.
        base_names: Optional model names for identity slices. Defaults to m0, m1, ...
    """
    n_models = len(per_model_preds)
    if n_models < 2:
        return {}

    if base_names is None:
        base_names = [f"m{i}" for i in range(n_models)]

    # Binary predictions from each model and from ensemble
    model_binary = np.array([(p >= 0.5).astype(int) for p in per_model_preds])
    ensemble_binary = (df["y_prob"].to_numpy() >= 0.5).astype(int)

    # Count how many models agree with ensemble per match
    agree_with_ensemble = np.sum(model_binary == ensemble_binary, axis=0)

    slices: dict[str, pl.DataFrame] = {}

    # Consensus count slices
    for n_agree in range(n_models, -1, -1):
        n_disagree = n_models - n_agree
        mask = agree_with_ensemble == n_agree
        if mask.any():
            slices[f"consensus:{n_agree}-{n_disagree}"] = df.filter(
                pl.Series(mask)
            )

    # Identity slices — only for non-unanimous predictions
    if n_models <= 7:  # avoid combinatorial explosion
        agrees_per_match = model_binary == ensemble_binary  # (n_models, n_matches)
        unanimous_mask = agree_with_ensemble == n_models

        if not unanimous_mask.all():
            labels = np.empty(len(df), dtype=object)
            labels[unanimous_mask] = None
            non_unanimous_idx = np.where(~unanimous_mask)[0]
            for idx in non_unanimous_idx:
                agreeing = sorted(
                    name for j, name in enumerate(base_names)
                    if agrees_per_match[j, idx]
                )
                labels[idx] = "+".join(agreeing)

            label_series = pl.Series("_cid", labels.tolist())
            df_with_label = df.with_columns(label_series)
            for label_val in label_series.drop_nulls().unique().to_list():
                filtered = df_with_label.filter(
                    pl.col("_cid") == label_val
                ).drop("_cid")
                if len(filtered) > 0:
                    slices[f"consensus_id:{label_val}"] = filtered

    return slices
