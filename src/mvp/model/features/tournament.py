"""Tournament-context features: within-tournament cumulative stats."""


import polars as pl

from mvp.model.registry import feature

DATE_COL = "effective_match_date"
TOURN_GROUP = ["player_id", "tournament_id", "year", "draw_type"]


def _sets_won() -> pl.Expr:
    """Count sets where player_set{i}_games > opp_set{i}_games across sets 1-5."""
    total = pl.lit(0)
    for i in range(1, 6):
        p = pl.col(f"player_set{i}_games")
        o = pl.col(f"opp_set{i}_games")
        total = total + (p > o).fill_null(False).cast(pl.Int64)
    return total


def _sets_lost() -> pl.Expr:
    """Count sets where opp_set{i}_games > player_set{i}_games across sets 1-5."""
    total = pl.lit(0)
    for i in range(1, 6):
        p = pl.col(f"player_set{i}_games")
        o = pl.col(f"opp_set{i}_games")
        total = total + (o > p).fill_null(False).cast(pl.Int64)
    return total


def _total_games_won() -> pl.Expr:
    """Sum of player_set{1-5}_games, null->0 for unplayed sets."""
    total = pl.lit(0)
    for i in range(1, 6):
        total = total + pl.col(f"player_set{i}_games").fill_null(0)
    return total


def _total_games_lost() -> pl.Expr:
    """Sum of opp_set{1-5}_games, null->0 for unplayed sets."""
    total = pl.lit(0)
    for i in range(1, 6):
        total = total + pl.col(f"opp_set{i}_games").fill_null(0)
    return total


def _tourn_cumulative(expr: pl.Expr) -> pl.Expr:
    """Cumulative sum shifted by 1, grouped by tournament context."""
    return (
        expr.cum_sum()
        .shift(1)
        .over(TOURN_GROUP, order_by=DATE_COL)
        .fill_null(0)
    )


# --- Base features ---


@feature(
    name="tourn_sets_won",
    params=[],
    description="Cumulative sets won in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_won() -> pl.Expr:
    return _tourn_cumulative(_sets_won())


@feature(
    name="tourn_sets_lost",
    params=[],
    description="Cumulative sets lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_lost() -> pl.Expr:
    return _tourn_cumulative(_sets_lost())


@feature(
    name="tourn_sets_margin",
    params=[],
    description="Cumulative sets won - lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_margin() -> pl.Expr:
    return _tourn_cumulative(_sets_won() - _sets_lost())


@feature(
    name="tourn_games_won",
    params=[],
    description="Cumulative games won in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_won() -> pl.Expr:
    return _tourn_cumulative(_total_games_won())


@feature(
    name="tourn_games_lost",
    params=[],
    description="Cumulative games lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_lost() -> pl.Expr:
    return _tourn_cumulative(_total_games_lost())


@feature(
    name="tourn_games_margin",
    params=[],
    description="Cumulative games won - lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_margin() -> pl.Expr:
    return _tourn_cumulative(_total_games_won() - _total_games_lost())


@feature(
    name="tourn_matches_won",
    params=[],
    description="Matches won in this tournament so far",
    mirror=True,
    impute=0,
)
def tourn_matches_won() -> pl.Expr:
    return _tourn_cumulative(pl.col("won").cast(pl.Int64))


# --- Derived diff features ---


@feature(
    name="tourn_sets_won_diff",
    params=[],
    description="Player - opponent cumulative sets won in tournament",
    depends_on=["tourn_sets_won"],
    mirror=False,
    impute=0,
)
def tourn_sets_won_diff() -> pl.Expr:
    return pl.col("player_tourn_sets_won") - pl.col("opp_tourn_sets_won")


@feature(
    name="tourn_sets_lost_diff",
    params=[],
    description="Player - opponent cumulative sets lost in tournament",
    depends_on=["tourn_sets_lost"],
    mirror=False,
    impute=0,
)
def tourn_sets_lost_diff() -> pl.Expr:
    return pl.col("player_tourn_sets_lost") - pl.col("opp_tourn_sets_lost")


@feature(
    name="tourn_sets_margin_diff",
    params=[],
    description="Player - opponent cumulative sets margin in tournament",
    depends_on=["tourn_sets_margin"],
    mirror=False,
    impute=0,
)
def tourn_sets_margin_diff() -> pl.Expr:
    return pl.col("player_tourn_sets_margin") - pl.col("opp_tourn_sets_margin")


@feature(
    name="tourn_games_won_diff",
    params=[],
    description="Player - opponent cumulative games won in tournament",
    depends_on=["tourn_games_won"],
    mirror=False,
    impute=0,
)
def tourn_games_won_diff() -> pl.Expr:
    return pl.col("player_tourn_games_won") - pl.col("opp_tourn_games_won")


@feature(
    name="tourn_games_lost_diff",
    params=[],
    description="Player - opponent cumulative games lost in tournament",
    depends_on=["tourn_games_lost"],
    mirror=False,
    impute=0,
)
def tourn_games_lost_diff() -> pl.Expr:
    return pl.col("player_tourn_games_lost") - pl.col("opp_tourn_games_lost")


@feature(
    name="tourn_games_margin_diff",
    params=[],
    description="Player - opponent cumulative games margin in tournament",
    depends_on=["tourn_games_margin"],
    mirror=False,
    impute=0,
)
def tourn_games_margin_diff() -> pl.Expr:
    return pl.col("player_tourn_games_margin") - pl.col("opp_tourn_games_margin")


@feature(
    name="tourn_matches_won_diff",
    params=[],
    description="Player - opponent matches won in tournament",
    depends_on=["tourn_matches_won"],
    mirror=False,
    impute=0,
)
def tourn_matches_won_diff() -> pl.Expr:
    return pl.col("player_tourn_matches_won") - pl.col("opp_tourn_matches_won")
