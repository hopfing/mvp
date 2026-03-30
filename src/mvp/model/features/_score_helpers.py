"""Shared helpers for set-level score expressions.

Used by score_depth, tournament, and tiebreak feature modules.
"""

import polars as pl

# --- Game counting ---


def total_games_won() -> pl.Expr:
    """Sum of player_set{1-5}_games, null->0 for unplayed sets."""
    total = pl.lit(0)
    for i in range(1, 6):
        total = total + pl.col(f"player_set{i}_games").fill_null(0)
    return total


def total_games_lost() -> pl.Expr:
    """Sum of opp_set{1-5}_games, null->0 for unplayed sets."""
    total = pl.lit(0)
    for i in range(1, 6):
        total = total + pl.col(f"opp_set{i}_games").fill_null(0)
    return total


# --- Set counting ---


def sets_won() -> pl.Expr:
    """Count sets where player_set{i}_games > opp_set{i}_games across sets 1-5."""
    total = pl.lit(0)
    for i in range(1, 6):
        p = pl.col(f"player_set{i}_games")
        o = pl.col(f"opp_set{i}_games")
        total = total + (p > o).fill_null(False).cast(pl.Int64)
    return total


def sets_lost() -> pl.Expr:
    """Count sets where opp_set{i}_games > player_set{i}_games across sets 1-5."""
    total = pl.lit(0)
    for i in range(1, 6):
        p = pl.col(f"player_set{i}_games")
        o = pl.col(f"opp_set{i}_games")
        total = total + (o > p).fill_null(False).cast(pl.Int64)
    return total


def is_straight_set_win() -> pl.Expr:
    """1 if player won in straight sets, 0 otherwise."""
    best_of = pl.col("best_of").fill_null(3)
    return (pl.col("won").cast(pl.Boolean) & (pl.col("sets_played") < best_of)).cast(pl.Int64)


# --- Tiebreak counting ---


def tiebreaks_played() -> pl.Expr:
    """Count tiebreaks played in this match."""
    return (
        pl.col("player_set1_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set2_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set3_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set4_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set5_tiebreak").is_not_null().cast(pl.Int64)
    )


def tiebreaks_won() -> pl.Expr:
    """Count tiebreaks won in this match."""
    total = pl.lit(0)
    for i in range(1, 6):
        won = (
            (pl.col(f"player_set{i}_tiebreak") > pl.col(f"opp_set{i}_tiebreak"))
            .fill_null(False)
            .cast(pl.Int64)
        )
        total = total + won
    return total


def deciding_set_flag() -> pl.Expr:
    """1 if this match went to a deciding set, 0 otherwise."""
    return (pl.col("sets_played") == pl.col("best_of")).cast(pl.Int64)
