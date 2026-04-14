"""Point-level derived features for the score-state serve model.

Pure functions that take the joined training DataFrame (points + match-level
features) and add derived columns. These are separate from the FeatureEngine
(which operates at match grain); score-state features operate on pre-point
score state carried in `match_beats_points.parquet`.

Each derivation is a named function returning a Polars expression. The set
of known derivations is exposed via `DERIVED_POINT_FEATURES`; configs
reference them by name in `point_level_features`.
"""

from collections.abc import Callable

import polars as pl


def _is_server_set_point() -> pl.Expr:
    """True if this is a set point AND the server is the one about to win the set."""
    server_at_game_point = (
        (pl.col("game_score_server") == "AD")
        | (
            (pl.col("game_score_server") == "40")
            & pl.col("game_score_returner").is_in(["0", "15", "30"])
        )
    )
    # is_set_point is only True when winning the current game wins the set.
    # Server's set-point = server game-point + is_set_point.
    return pl.col("is_set_point") & server_at_game_point


def _is_returner_set_point() -> pl.Expr:
    returner_at_game_point = (
        (pl.col("game_score_returner") == "AD")
        | (
            (pl.col("game_score_returner") == "40")
            & pl.col("game_score_server").is_in(["0", "15", "30"])
        )
    )
    return pl.col("is_set_point") & returner_at_game_point


def _is_server_match_point() -> pl.Expr:
    server_at_game_point = (
        (pl.col("game_score_server") == "AD")
        | (
            (pl.col("game_score_server") == "40")
            & pl.col("game_score_returner").is_in(["0", "15", "30"])
        )
    )
    return pl.col("is_match_point") & server_at_game_point


def _is_returner_match_point() -> pl.Expr:
    returner_at_game_point = (
        (pl.col("game_score_returner") == "AD")
        | (
            (pl.col("game_score_returner") == "40")
            & pl.col("game_score_server").is_in(["0", "15", "30"])
        )
    )
    return pl.col("is_match_point") & returner_at_game_point


def _set_score_asymmetry() -> pl.Expr:
    """Server's games lead in the current set (negative = server trailing)."""
    return (pl.col("set_score_server_games") - pl.col("set_score_returner_games")).cast(pl.Int64)


def _sets_won_asymmetry() -> pl.Expr:
    """Server's sets-won lead in the match (negative = server trailing)."""
    return (pl.col("sets_won_server") - pl.col("sets_won_returner")).cast(pl.Int64)


def _is_second_serve() -> pl.Expr:
    return pl.col("serve") == 2


def _game_score_numeric_server() -> pl.Expr:
    """Server's game score as numeric (0, 15, 30, 40, 45[D], 50[AD])."""
    return (
        pl.when(pl.col("game_score_server") == "0").then(0)
        .when(pl.col("game_score_server") == "15").then(15)
        .when(pl.col("game_score_server") == "30").then(30)
        .when(pl.col("game_score_server") == "40").then(40)
        .when(pl.col("game_score_server") == "D").then(45)
        .when(pl.col("game_score_server") == "AD").then(50)
        .otherwise(None)
        .cast(pl.Int64)
    )


def _game_score_numeric_returner() -> pl.Expr:
    return (
        pl.when(pl.col("game_score_returner") == "0").then(0)
        .when(pl.col("game_score_returner") == "15").then(15)
        .when(pl.col("game_score_returner") == "30").then(30)
        .when(pl.col("game_score_returner") == "40").then(40)
        .when(pl.col("game_score_returner") == "D").then(45)
        .when(pl.col("game_score_returner") == "AD").then(50)
        .otherwise(None)
        .cast(pl.Int64)
    )


def _game_score_diff() -> pl.Expr:
    """Numeric game-score lead of server within current game (- = trailing)."""
    return _game_score_numeric_server() - _game_score_numeric_returner()


def _is_surface_hard() -> pl.Expr:
    return pl.col("surface") == "hard"


def _is_surface_clay() -> pl.Expr:
    return pl.col("surface") == "clay"


def _is_surface_grass() -> pl.Expr:
    return pl.col("surface") == "grass"


DERIVED_POINT_FEATURES: dict[str, Callable[[], pl.Expr]] = {
    # Score-state split by whose side it is
    "is_server_set_point": _is_server_set_point,
    "is_returner_set_point": _is_returner_set_point,
    "is_server_match_point": _is_server_match_point,
    "is_returner_match_point": _is_returner_match_point,
    # Asymmetries
    "set_score_asymmetry": _set_score_asymmetry,
    "sets_won_asymmetry": _sets_won_asymmetry,
    "game_score_diff": _game_score_diff,
    # Raw convenience
    "is_second_serve": _is_second_serve,
    "game_score_numeric_server": _game_score_numeric_server,
    "game_score_numeric_returner": _game_score_numeric_returner,
    # Surface one-hots
    "is_surface_hard": _is_surface_hard,
    "is_surface_clay": _is_surface_clay,
    "is_surface_grass": _is_surface_grass,
}


# Raw point-level columns from match_beats_points.parquet that are usable as
# model features directly (boolean or numeric). Exposed here so the default
# "full pool" expansion in serve_discovery can pick them up without hard-coding.
RAW_POINT_FEATURES: tuple[str, ...] = (
    "is_break_point",
    "is_set_point",
    "is_match_point",
    "is_tiebreak",
    "set_score_server_games",
    "set_score_returner_games",
    "sets_won_server",
    "sets_won_returner",
    "serve",
    "set_num",
    "game_num",
    "point_num",
)


def default_point_level_candidate_pool() -> list[str]:
    """Full pool of point-level candidate features (raw + derived).

    Mirrors the "empty include → iterate registered features" convention
    used by classification / projection FS.
    """
    return list(RAW_POINT_FEATURES) + list(DERIVED_POINT_FEATURES.keys())


def add_derived_point_features(df: pl.DataFrame, feature_names: list[str]) -> pl.DataFrame:
    """Add the requested derived columns to `df`. Unknown names raise ValueError.

    Only names not already present as columns are computed (allows mixing raw
    match_beats_points columns like `is_break_point` and derived names like
    `is_server_set_point` in the same list without conflict).
    """
    existing = set(df.columns)
    exprs: list[pl.Expr] = []
    for name in feature_names:
        if name in existing:
            continue
        if name not in DERIVED_POINT_FEATURES:
            raise ValueError(
                f"unknown point-level feature '{name}'. "
                f"Known: {sorted(DERIVED_POINT_FEATURES.keys())}"
            )
        exprs.append(DERIVED_POINT_FEATURES[name]().alias(name))
    if exprs:
        df = df.with_columns(exprs)
    return df
