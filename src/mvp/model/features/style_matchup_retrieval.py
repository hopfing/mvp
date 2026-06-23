"""Style-matchup lookup — Form A (joint kNN retrieval).

For match A vs B, weight each of A's PAST matches by a Gaussian kernel on the
distance between that past opponent's radar (at the time) and B's radar (now),
and aggregate A's rating-residual:

    feat_A = Σ_O w_O·resid_AO / (Σ_O w_O + λ)        (ridge λ — review F2)
    w_O    = exp(−‖radar_O@t_O − radar_B@t‖² / 2h²)  (Gaussian, h)
    N_eff_A = (Σ w_O)² / Σ w_O²

The weight depends on the CURRENT match's opponent B, so this can't be a rolling
`@feature` — it's a within-player self-join (current row × A's prior rows,
filtered to strictly-prior and a window W). resid = won − Elo-implied(surface)
win prob (the Form-B residual). Leakage-safe: the pool is `t_p < t_m` only.

This is the transform; wiring it into the feature pipeline (precompute → table,
or post-engine) is the §4 integration step. Match-level combination
(feat_A − feat_B, separate confs) via `combine_match_level`.
"""

from pathlib import Path

import polars as pl

_GRP = "player_id"
_DATE = "effective_match_date"
_AXES = ["serve", "net", "aggression", "error", "rally"]

# Kernel bandwidth (spec D-KERNEL). Recalibrated from 1.0 to the radar's
# EMPIRICAL scale: the centered+shrunk radar has per-axis std ~0.15-0.4 (not the
# unit-normal the 1.0 assumed), so typical pairwise distance ≈0.8 not √5. At
# h=0.3 the kernel discriminates (n_eff median ≈26) vs h=1.0 collapsing toward
# the unconditional residual (n_eff ≈70). Key sweep parameter.
H_DEFAULT = 0.3
LAMBDA_DEFAULT = 1.0  # ridge on the slope denominator (review F2)
WINDOW_DAYS_DEFAULT = 1095  # pool window (spec D-POOL)


def _residual() -> pl.Expr:
    e = 1.0 / (1.0 + 10.0 ** (-pl.col("player_elo_surface_diff") / 400.0))
    return pl.col("won").cast(pl.Float64) - e


def compute_form_a(
    df: pl.DataFrame,
    h: float = H_DEFAULT,
    lam: float = LAMBDA_DEFAULT,
    window_days: int = WINDOW_DAYS_DEFAULT,
) -> pl.DataFrame:
    """Per (match_uid, player_id): feat_a, n_eff_a, mean_opp_rating.

    `df` must carry: player_id, match_uid, effective_match_date,
    opp_style_radar_{axis} (the opponent's radar this match), won,
    player_elo_surface_diff, opp_elo. Leakage-safe (pool = strictly-prior).
    """
    cur = df.select(
        _GRP, "opp_id", "match_uid",
        pl.col(_DATE).alias("t_m"),
        *[pl.col(f"opp_style_radar_{k}").alias(f"b_{k}") for k in _AXES],
    )
    pool = df.select(
        _GRP,
        pl.col(_DATE).alias("t_p"),
        *[pl.col(f"opp_style_radar_{k}").alias(f"o_{k}") for k in _AXES],
        _residual().alias("resid"),
        pl.col("opp_elo").alias("opp_elo"),
        # pool opponent's worst-spoke radar confidence (F8 diagnostic input)
        pl.min_horizontal([pl.col(f"opp_style_conf_{k}") for k in _AXES]).alias("minconf"),
    )

    pairs = cur.join(pool, on=_GRP).filter(
        (pl.col("t_p") < pl.col("t_m"))
        & (pl.col("t_p") >= pl.col("t_m") - pl.duration(days=window_days))
    )
    # squared radar distance, ‖b − o‖² (explicit accumulation, codebase convention)
    dist2 = (pl.col(f"b_{_AXES[0]}") - pl.col(f"o_{_AXES[0]}")) ** 2
    for k in _AXES[1:]:
        dist2 = dist2 + (pl.col(f"b_{k}") - pl.col(f"o_{k}")) ** 2
    pairs = pairs.with_columns((-dist2 / (2.0 * h * h)).exp().alias("w"))

    agg = pairs.group_by("match_uid", _GRP).agg(
        pl.col("opp_id").first(),  # constant within the (match_uid, player_id) group
        sw=pl.col("w").sum(),
        swr=(pl.col("w") * pl.col("resid")).sum(),
        sw2=(pl.col("w") ** 2).sum(),
        swrat=(pl.col("w") * pl.col("opp_elo")).sum(),
        swc=(pl.col("w") * pl.col("minconf")).sum(),
    )
    return agg.select(
        "match_uid", _GRP, "opp_id",
        (pl.col("swr") / (pl.col("sw") + lam)).alias("feat_a"),  # ridge (F2)
        pl.when(pl.col("sw2") > 0).then(pl.col("sw") ** 2 / pl.col("sw2"))
            .otherwise(None).alias("n_eff_a"),
        pl.when(pl.col("sw") > 0).then(pl.col("swrat") / pl.col("sw"))
            .otherwise(None).alias("mean_opp_rating"),
        # kernel-weighted pool radar quality (F8): low => shrinkage-homogenized pool
        pl.when(pl.col("sw") > 0).then(pl.col("swc") / pl.col("sw"))
            .otherwise(None).alias("mean_pool_conf"),
    )


def combine_match_level(form_a: pl.DataFrame) -> pl.DataFrame:
    """Per (match_uid, player_id): style_matchup_residual = feat_a − opp.feat_a,
    carrying both N_eff sides + the min (review F3)."""
    opp = form_a.select(
        "match_uid",
        pl.col(_GRP).alias("_opp_pid"),
        pl.col("feat_a").alias("feat_b"),
        pl.col("n_eff_a").alias("n_eff_b"),
    )
    # pair this row to its opponent's row precisely: this.opp_id == opp.player_id
    # (robust to anything but a clean 2-perspective match_uid).
    m = form_a.join(
        opp, left_on=["match_uid", "opp_id"], right_on=["match_uid", "_opp_pid"], how="inner",
    )
    return m.select(
        "match_uid", _GRP,
        (pl.col("feat_a") - pl.col("feat_b")).alias("style_matchup_residual"),
        pl.col("n_eff_a").alias("style_matchup_n_eff_a"),
        pl.col("n_eff_b").alias("style_matchup_n_eff_b"),
        pl.min_horizontal("n_eff_a", "n_eff_b").alias("style_matchup_conf"),
        pl.col("mean_opp_rating").alias("mean_opp_rating_pool"),
        pl.col("mean_pool_conf").alias("style_matchup_pool_conf"),
    )


def build_style_matchup_table(
    matches_path: str | Path,
    out_path: str | Path,
    cache_dir: str | Path,
    h: float = H_DEFAULT,
    lam: float = LAMBDA_DEFAULT,
    window_days: int = WINDOW_DAYS_DEFAULT,
) -> pl.DataFrame:
    """Materialize the radar over `matches_path`, run the Form A self-join, and
    write the matchup feature table (one row per (match_uid, player_id)) to
    `out_path` — piece 1 of the integration (the table the passthrough layer loads).

    The caller scopes `matches_path` (full history, or a window whose earliest
    matches accept a truncated pool). Heavy over full history (radar chain +
    self-join) — a periodic offline job, not the 15-min tick.
    """
    # Engine imported here, not at module top: the transform functions above are
    # pure-polars and importable standalone; only this build entrypoint needs the
    # (heavy) feature engine.
    from mvp.model.engine import FeatureEngine

    eng = FeatureEngine(matches_path=Path(matches_path), cache_dir=Path(cache_dir))
    feats = (
        [f"opp_style_radar_{k}" for k in _AXES]
        + [f"opp_style_conf_{k}" for k in _AXES]
        + ["player_elo_surface_diff"]
    )
    df = eng.compute(feats, extra_columns=["won", "opp_elo"])
    table = combine_match_level(compute_form_a(df, h=h, lam=lam, window_days=window_days))

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    table.write_parquet(out)
    return table
