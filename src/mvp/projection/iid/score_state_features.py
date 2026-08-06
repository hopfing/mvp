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

from mvp.projection.iid.score_state import GAME_SCORE_POINTS


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


def _points_won(col: str) -> pl.Expr:
    """Points won in the current game by the side named in `col`.

    One scale for both formats, per `GAME_SCORE_POINTS`. This replaces a
    display-score mapping (0/15/30/40/45/50) that enumerated only regular-game
    labels and sent everything else to null — which, on the 229,682 tiebreak
    points where the label is a raw count, meant 91.5% of `game_score_diff` was
    null. Those nulls were not inert: the logistic path drops non-finite rows,
    so tiebreak points left training entirely, and the two counts that DID
    parse ("0" and "15") landed on the regular-game values 0 and 15, encoding a
    tiebreak at fifteen points identically to a game at 15-love.
    """
    tiebreak = pl.col("is_tiebreak").fill_null(False)
    expr = pl.when(tiebreak).then(pl.col(col).cast(pl.Int64, strict=False))
    for label, pts in GAME_SCORE_POINTS.items():
        expr = expr.when(pl.col(col) == label).then(pl.lit(pts, dtype=pl.Int64))
    # Still null for a label in neither vocabulary — a genuinely unreadable
    # score, which is rare (~0.01% of regular points) and honestly unknown.
    return expr.otherwise(pl.lit(None, dtype=pl.Int64)).cast(pl.Int64)


def _game_points_server() -> pl.Expr:
    return _points_won("game_score_server")


def _game_points_returner() -> pl.Expr:
    return _points_won("game_score_returner")


def _game_points_diff() -> pl.Expr:
    """Server's points lead within the current game (negative = trailing)."""
    return _game_points_server() - _game_points_returner()


def _tiebreak_point_diff() -> pl.Expr:
    """Server's tiebreak lead, 0 outside a tiebreak.

    Zero off-tiebreak rather than null so the feature is an interaction rather
    than a hole: the coefficient acts only on tiebreak points, which is the
    tiebreak-specific slope a linear model cannot otherwise express, and it
    costs no rows in a path that drops non-finite ones.
    """
    return (
        pl.when(pl.col("is_tiebreak").fill_null(False))
        .then(_game_points_diff())
        .otherwise(pl.lit(0, dtype=pl.Int64))
        .fill_null(0)
        .cast(pl.Int64)
    )


def _tiebreak_points_played() -> pl.Expr:
    """Points completed in the current tiebreak, 0 outside one."""
    return (
        pl.when(pl.col("is_tiebreak").fill_null(False))
        .then(_game_points_server() + _game_points_returner())
        .otherwise(pl.lit(0, dtype=pl.Int64))
        .fill_null(0)
        .cast(pl.Int64)
    )


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
    "game_points_diff": _game_points_diff,
    # Tiebreak state. Separate from the game-points columns above so a linear
    # model gets a tiebreak-specific slope; a tree can reach the same thing by
    # splitting on is_tiebreak, and gets these for free either way.
    "tiebreak_point_diff": _tiebreak_point_diff,
    "tiebreak_points_played": _tiebreak_points_played,
    # Raw convenience
    "is_second_serve": _is_second_serve,
    "game_points_server": _game_points_server,
    "game_points_returner": _game_points_returner,
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
