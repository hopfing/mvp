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

Match-level combination (feat_A − feat_B, separate confs) via `combine_match_level`.
It's wired into the engine as a `register_transform` feature kind (engine-computed,
cached/invalidated like any feature — no separate synced table); `build_style_
matchup_table` remains the standalone offline/verification path.
"""

from pathlib import Path

import polars as pl

from mvp.model.registry import register_transform

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
# Players per self-join chunk — bounds the transient `pairs` frame so the
# full-history retrieval stays under the engine memory guard. Within-player, so
# chunking is exact (no cross-batch pairs).
_PLAYER_BATCH = 500


def _residual() -> pl.Expr:
    e = 1.0 / (1.0 + 10.0 ** (-pl.col("player_elo_surface_diff") / 400.0))
    return pl.col("won").cast(pl.Float64) - e


def _form_a_core(
    df: pl.DataFrame,
    h: float = H_DEFAULT,
    lam: float = LAMBDA_DEFAULT,
    window_days: int | None = WINDOW_DAYS_DEFAULT,
) -> pl.DataFrame:
    """Per (match_uid, player_id): feat_a, n_eff_a, mean_opp_rating, mean_pool_conf.

    `df` must carry: player_id, match_uid, effective_match_date,
    opp_style_radar_{axis} (the opponent's radar this match), opp_style_conf_{axis},
    won, player_elo_surface_diff, opp_elo. Leakage-safe (pool = strictly-prior).
    The retrieval is within-player, so this is exact on any player-complete subset
    of `df` — which is what lets `compute_form_a` chunk it by player.

    ``window_days=None`` = alltime pool (all strictly-prior matches); ``N`` =
    the [t_m − N, t_m) pool. The ``t_p < t_m`` leakage guard is independent of it.
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

    prior = pl.col("t_p") < pl.col("t_m")  # leakage guard (always)
    if window_days is not None:
        prior = prior & (pl.col("t_p") >= pl.col("t_m") - pl.duration(days=window_days))
    pairs = cur.join(pool, on=_GRP).filter(prior)
    # squared radar distance, ‖b − o‖² (explicit accumulation, codebase convention)
    dist2 = (pl.col(f"b_{_AXES[0]}") - pl.col(f"o_{_AXES[0]}")) ** 2
    for k in _AXES[1:]:
        dist2 = dist2 + (pl.col(f"b_{k}") - pl.col(f"o_{k}")) ** 2
    pairs = pairs.with_columns((-dist2 / (2.0 * h * h)).exp().alias("w"))
    # Drop pairs with no residual signal (null Elo baseline, e.g. a pool match
    # before the opponent had a rating) or a null radar, so every aggregate below
    # shares ONE support. Otherwise a null-resid pair adds weight to the kernel
    # denominator (Σw) without contributing to the numerator (Σw·resid) — diluting
    # feat_a — and the flat control's pl.len() would count it too, so the flat
    # would not equal the kernel's h->inf limit.
    pairs = pairs.filter(pl.col("resid").is_not_null() & pl.col("w").is_not_null())

    agg = pairs.group_by("match_uid", _GRP).agg(
        pl.col("opp_id").first(),  # constant within the (match_uid, player_id) group
        sw=pl.col("w").sum(),
        swr=(pl.col("w") * pl.col("resid")).sum(),
        sw2=(pl.col("w") ** 2).sum(),
        swrat=(pl.col("w") * pl.col("opp_elo")).sum(),
        swc=(pl.col("w") * pl.col("minconf")).sum(),
        # flat (h->inf) control: uniform weights over the SAME pairs — the
        # unconditional pool-mean residual, kernel/style switched off.
        sr=pl.col("resid").sum(),
        npairs=pl.len(),
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
        # flat control: same ridge, uniform weights (style-blind)
        (pl.col("sr") / (pl.col("npairs") + lam)).alias("feat_a_flat"),
    )


def compute_form_a(
    df: pl.DataFrame,
    h: float = H_DEFAULT,
    lam: float = LAMBDA_DEFAULT,
    window_days: int | None = WINDOW_DAYS_DEFAULT,
    player_batch_size: int | None = _PLAYER_BATCH,
) -> pl.DataFrame:
    """`_form_a_core` chunked by player to bound the self-join's peak memory.

    The retrieval is within-player (the join is on `player_id`), so processing a
    disjoint subset of players is exact — each batch's `pairs` frame is bounded,
    and the per-batch aggregates concatenate with no cross-batch interaction.
    `player_batch_size=None` runs the whole frame at once (small inputs / tests).
    """
    if player_batch_size is None:
        return _form_a_core(df, h, lam, window_days)
    players = df.get_column(_GRP).unique().to_list()
    parts = [
        _form_a_core(
            df.filter(pl.col(_GRP).is_in(players[i : i + player_batch_size])),
            h, lam, window_days,
        )
        for i in range(0, len(players), player_batch_size)
    ]
    return pl.concat(parts) if parts else _form_a_core(df, h, lam, window_days)


def combine_match_level(form_a: pl.DataFrame) -> pl.DataFrame:
    """Assemble the Form A output frame per (match_uid, player_id): the row-
    player's quantities (`player_…`), the opponent's (`opp_…`, paired via the
    self-join), and the player−opp `…_diff` for the two signal columns.

    Each column is a member of the `vs_opp_style_*` family — the row-player's
    residual against opponents who play like *their* current opponent. The
    transform self-emits both perspectives (the engine doesn't mirror transform
    outputs), and a LEFT join keeps the row-player's value even when the opponent
    has no pool (the `opp_`/`_diff` are then null).
    """
    opp = form_a.select(
        "match_uid",
        pl.col(_GRP).alias("_opp_pid"),
        pl.col("feat_a").alias("feat_b"),
        pl.col("feat_a_flat").alias("feat_b_flat"),
        pl.col("n_eff_a").alias("n_eff_b"),
        pl.col("mean_opp_rating").alias("opp_rating_b"),
        pl.col("mean_pool_conf").alias("pool_conf_b"),
    )
    # pair this row to its opponent's row precisely: this.opp_id == opp.player_id.
    m = form_a.join(
        opp, left_on=["match_uid", "opp_id"], right_on=["match_uid", "_opp_pid"], how="left",
    )
    return m.select(
        "match_uid", _GRP,
        # residual (the signal): player, opp, diff (diff is player-perspective,
        # mirror=False convention -> player_ prefix, like player_elo_surface_diff)
        pl.col("feat_a").alias("player_vs_opp_style_resid"),
        pl.col("feat_b").alias("opp_vs_opp_style_resid"),
        (pl.col("feat_a") - pl.col("feat_b")).alias("player_vs_opp_style_resid_diff"),
        # flat (h->inf, style-blind) control: player, opp, diff
        pl.col("feat_a_flat").alias("player_vs_opp_style_resid_flat"),
        pl.col("feat_b_flat").alias("opp_vs_opp_style_resid_flat"),
        (pl.col("feat_a_flat") - pl.col("feat_b_flat")).alias("player_vs_opp_style_resid_flat_diff"),
        # style-specific excess (kernel − flat): the matchup signal net of general
        # form — the purest non-transitivity component. player, opp, diff.
        (pl.col("feat_a") - pl.col("feat_a_flat")).alias("player_vs_opp_style_resid_xs"),
        (pl.col("feat_b") - pl.col("feat_b_flat")).alias("opp_vs_opp_style_resid_xs"),
        ((pl.col("feat_a") - pl.col("feat_a_flat"))
         - (pl.col("feat_b") - pl.col("feat_b_flat"))).alias("player_vs_opp_style_resid_xs_diff"),
        # effective pool size (reliability): player, opp
        pl.col("n_eff_a").alias("player_vs_opp_style_neff"),
        pl.col("n_eff_b").alias("opp_vs_opp_style_neff"),
        # mean pool-opponent Elo (strength confound): player, opp
        pl.col("mean_opp_rating").alias("player_vs_opp_style_pool_elo"),
        pl.col("opp_rating_b").alias("opp_vs_opp_style_pool_elo"),
        # pool radar quality (F8): player, opp
        pl.col("mean_pool_conf").alias("player_vs_opp_style_pool_conf"),
        pl.col("pool_conf_b").alias("opp_vs_opp_style_pool_conf"),
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


# --- engine-computed transform feature (Form A, config-selectable) ---

_OUTPUTS = [
    # residual (the signal) + flat (h->inf) control: per-player + diff
    "player_vs_opp_style_resid", "opp_vs_opp_style_resid", "player_vs_opp_style_resid_diff",
    "player_vs_opp_style_resid_flat", "opp_vs_opp_style_resid_flat",
    "player_vs_opp_style_resid_flat_diff",
    # style-specific excess (kernel - flat): per-player + diff
    "player_vs_opp_style_resid_xs", "opp_vs_opp_style_resid_xs",
    "player_vs_opp_style_resid_xs_diff",
    # reliability / pool descriptors: per-player
    "player_vs_opp_style_neff", "opp_vs_opp_style_neff",
    "player_vs_opp_style_pool_elo", "opp_vs_opp_style_pool_elo",
    "player_vs_opp_style_pool_conf", "opp_vs_opp_style_pool_conf",
]


def _assert_radar_as_of_time(df: pl.DataFrame) -> None:
    """[F1] defensive guard against a radar-leakage regression.

    The rigorous closed-left guarantee lives in the radar code (`_roll730` /
    `_eff_n` use `closed="left"`, reviewed) and the `t_p < t_m` pool filter
    (verify_matchup_a). This is a cheap sample check that the radar columns are
    materialized and non-degenerate — catching a gross break (all-null or a
    constant radar) that would silently invalidate the leakage story.
    """
    col = f"opp_style_radar_{_AXES[0]}"
    s = df.get_column(col)
    if s.null_count() == s.len():
        raise ValueError(f"[F1] {col} entirely null — radar not materialized")
    if s.drop_nulls().n_unique() <= 1:
        raise ValueError(f"[F1] {col} constant — radar not rolling (possible leak)")


def _matchup_transform(df: pl.DataFrame, days: int | None = None) -> pl.DataFrame:
    """Engine transform: the Form A style-matchup output frame (one row per
    match_uid/player_id, the bare `_OUTPUTS` columns) from the within-player kNN
    self-join. The engine renames the outputs per the days variant (`_{N}d`) and
    merges/caches them — so this returns just the keyed outputs, not the matrix.

    ``days`` is the pool lookback (Window 2): ``None`` = alltime, ``N`` = N-day.
    """
    _assert_radar_as_of_time(df)
    return combine_match_level(compute_form_a(df, window_days=days))


register_transform(
    name="style_matchup",
    func=_matchup_transform,
    outputs=_OUTPUTS,
    params=["days"],  # Window 2 (pool lookback) — swept via window_sizes
    # base names — the engine resolves each to player_ + opp_ (the func reads the
    # opp_ radar/conf and player_elo_surface_diff).
    depends_on=(
        [f"style_radar_{k}" for k in _AXES]
        + [f"style_conf_{k}" for k in _AXES]
        + ["elo_surface_diff"]
    ),
    raw_columns=["opp_elo"],
    description="Form A joint-kNN style-matchup retrieval (whole-matrix self-join)",
)
