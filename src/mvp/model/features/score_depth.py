"""Score-depth features: how a player is winning (rolling window)."""


import polars as pl

from mvp.model.registry import feature

DATE_COL = "effective_match_date"


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


def _is_straight_set_win() -> pl.Expr:
    """1 if player won in straight sets, 0 otherwise."""
    best_of = pl.col("best_of").fill_null(3)
    return (pl.col("won").cast(pl.Boolean) & (pl.col("sets_played") < best_of)).cast(pl.Int64)


def _games_won_per_set() -> pl.Expr:
    """Games won per set in this match."""
    return _total_games_won().cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_lost_per_set() -> pl.Expr:
    """Games lost per set in this match."""
    return _total_games_lost().cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_margin_per_set() -> pl.Expr:
    """(Games won - games lost) per set in this match."""
    return (_total_games_won() - _total_games_lost()).cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_per_set() -> pl.Expr:
    """Total games per set in this match."""
    return (_total_games_won() + _total_games_lost()).cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _rolling_ratio(numerator: pl.Expr, denominator: pl.Expr, days: int) -> pl.Expr:
    """Rolling sum(numerator) / rolling sum(denominator) over past N days."""
    num = numerator.rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id").fill_null(0)
    den = denominator.rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id").fill_null(0)
    return pl.when(den > 0).then(num / den).otherwise(None)


def _cumulative_ratio(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """Cumulative sum(numerator) / cumulative sum(denominator), excluding current row."""
    num = numerator.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    den = denominator.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(den > 0).then(num / den).otherwise(None)


def _rolling_mean(expr: pl.Expr, days: int) -> pl.Expr:
    """Rolling mean of an expression over past N days."""
    return expr.rolling_mean_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id")


def _cumulative_mean(expr: pl.Expr) -> pl.Expr:
    """Cumulative mean of an expression, excluding current row."""
    cum_s = expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL)
    cum_c = (
        pl.col(DATE_COL).is_not_null().cast(pl.Int64)
        .cum_sum().shift(1).over("player_id", order_by=DATE_COL)
    )
    return cum_s / cum_c


# --- Base features ---


@feature(
    name="sets_per_match",
    params=["days"],
    description="Avg sets played per match in window",
    mirror=True,
    impute="median",
)
def sets_per_match(days: int | None = None) -> pl.Expr:
    expr = pl.col("sets_played").cast(pl.Float64)
    if days is None:
        return _cumulative_mean(expr)
    return _rolling_mean(expr, days)


@feature(
    name="straight_sets_win_pct",
    params=["days"],
    description="Fraction of wins in straight sets",
    mirror=True,
    impute=0.5,
)
def straight_sets_win_pct(days: int | None = None) -> pl.Expr:
    wins = pl.col("won").cast(pl.Int64)
    ss_wins = _is_straight_set_win()
    if days is None:
        return _cumulative_ratio(ss_wins, wins)
    return _rolling_ratio(ss_wins, wins, days)


@feature(
    name="games_won_per_set",
    params=["days"],
    description="Avg games won per set in window",
    mirror=True,
    impute="median",
)
def games_won_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return _cumulative_mean(_games_won_per_set())
    return _rolling_mean(_games_won_per_set(), days)


@feature(
    name="games_lost_per_set",
    params=["days"],
    description="Avg games lost per set in window",
    mirror=True,
    impute="median",
)
def games_lost_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return _cumulative_mean(_games_lost_per_set())
    return _rolling_mean(_games_lost_per_set(), days)


@feature(
    name="games_margin_per_set",
    params=["days"],
    description="Avg (games won - lost) per set in window",
    mirror=True,
    impute="median",
)
def games_margin_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return _cumulative_mean(_games_margin_per_set())
    return _rolling_mean(_games_margin_per_set(), days)


@feature(
    name="games_per_set",
    params=["days"],
    description="Avg total games per set in window (tightness)",
    mirror=True,
    impute="median",
)
def games_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return _cumulative_mean(_games_per_set())
    return _rolling_mean(_games_per_set(), days)


# --- Derived diff features ---


@feature(
    name="sets_per_match_diff",
    params=["days"],
    description="Player - opponent sets per match",
    depends_on=["sets_per_match"],
    mirror=False,
    impute=0,
)
def sets_per_match_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_sets_per_match") - pl.col("opp_sets_per_match")
    return pl.col(f"player_sets_per_match_{days}d") - pl.col(f"opp_sets_per_match_{days}d")


@feature(
    name="straight_sets_win_pct_diff",
    params=["days"],
    description="Player - opponent straight sets win pct",
    depends_on=["straight_sets_win_pct"],
    mirror=False,
    impute=0,
)
def straight_sets_win_pct_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_straight_sets_win_pct") - pl.col("opp_straight_sets_win_pct")
    return pl.col(f"player_straight_sets_win_pct_{days}d") - pl.col(f"opp_straight_sets_win_pct_{days}d")


@feature(
    name="games_won_per_set_diff",
    params=["days"],
    description="Player - opponent games won per set",
    depends_on=["games_won_per_set"],
    mirror=False,
    impute=0,
)
def games_won_per_set_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_games_won_per_set") - pl.col("opp_games_won_per_set")
    return pl.col(f"player_games_won_per_set_{days}d") - pl.col(f"opp_games_won_per_set_{days}d")


@feature(
    name="games_lost_per_set_diff",
    params=["days"],
    description="Player - opponent games lost per set",
    depends_on=["games_lost_per_set"],
    mirror=False,
    impute=0,
)
def games_lost_per_set_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_games_lost_per_set") - pl.col("opp_games_lost_per_set")
    return pl.col(f"player_games_lost_per_set_{days}d") - pl.col(f"opp_games_lost_per_set_{days}d")


@feature(
    name="games_margin_per_set_diff",
    params=["days"],
    description="Player - opponent games margin per set",
    depends_on=["games_margin_per_set"],
    mirror=False,
    impute=0,
)
def games_margin_per_set_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_games_margin_per_set") - pl.col("opp_games_margin_per_set")
    return pl.col(f"player_games_margin_per_set_{days}d") - pl.col(f"opp_games_margin_per_set_{days}d")


@feature(
    name="games_per_set_diff",
    params=["days"],
    description="Player - opponent games per set",
    depends_on=["games_per_set"],
    mirror=False,
    impute=0,
)
def games_per_set_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_games_per_set") - pl.col("opp_games_per_set")
    return pl.col(f"player_games_per_set_{days}d") - pl.col(f"opp_games_per_set_{days}d")
