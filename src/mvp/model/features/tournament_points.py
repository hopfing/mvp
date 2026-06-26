"""In-tournament (current-year) point / game / set features.

The point-level analogue of the cumulative `tourn_*` block in ``tournament.py``:
serve / return / point / game / set outcomes accumulated within the CURRENT
running of a tournament (``cum_sum`` + ``shift(1)``, temporally safe), grouped by
``[player_id, tournament_id, year, draw_type]``. Distinct from the
``tourn_history_*`` block (cross-year iterations of the same tournament).

Spec: ``mvp-docs/specs/2026-06-26-in-tournament-point-game-features.md``.

Two correctness rules (see spec §5):
  * Per-set / per-match rates mask the denominator to matches where the ratio's
    OWN numerator is non-null, so stats-less (futures / pre-2003 / partial) matches
    don't dilute the rate. Per-game rates use a domain-matched stats-feed game
    count. Implemented in ``_rate``.
  * Games won decompose into three parts: holds + breaks + tiebreaks_won
    (verified to reconcile to the score at 99.5%).
"""

import polars as pl

from mvp.model.features._score_helpers import (
    blowout_sets as _blowout_sets,
    deciding_set_flag as _deciding_set_flag,
    is_straight_set_win as _is_straight_set_win,
    sets_lost as _sets_lost,
    sets_won as _sets_won,
    tiebreaks_played as _tiebreaks_played,
    tiebreaks_won as _tiebreaks_won,
    tight_sets as _tight_sets,
)
from mvp.model.registry import feature, register_diff, register_matchup

DATE_COL = "effective_match_date"
TOURN_GROUP = ["player_id", "tournament_id", "year", "draw_type"]
ORDER = [DATE_COL, "tournament_start_date", "round_order", "match_uid"]


# --- accumulation helpers ----------------------------------------------------


def _cum(expr: pl.Expr) -> pl.Expr:
    """Cumulative sum over prior in-tournament matches (current excluded).

    No fill: the first match of a tournament is null (no prior history), carried
    to the NaN-tolerant model via ``impute=None``.
    """
    return expr.cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER)


def _rate(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    """In-tournament cumulative ratio, masked so stats-less matches can't dilute.

    Both numerator and denominator accumulate only over matches where the
    numerator is present (and the denominator too), then divide; null until the
    accumulated denominator is positive. Mirrors ``cumulative_mean``'s per-value
    valid-mask bookkeeping (spec §5a).
    """
    valid = num.is_not_null() & den.is_not_null()
    num_m = pl.when(valid).then(num.cast(pl.Float64)).otherwise(0.0)
    den_m = pl.when(valid).then(den.cast(pl.Float64)).otherwise(0.0)
    cnum = num_m.cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER)
    cden = den_m.cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER)
    return pl.when(cden > 0).then(cnum / cden).otherwise(None)


def _mean(col: str) -> pl.Expr:
    """In-tournament cumulative mean (null source rows excluded from both moments)."""
    x = pl.col(col)
    valid = x.is_not_null().cast(pl.Float64)
    cs = x.fill_null(0.0).cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER)
    cc = valid.cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER)
    return pl.when(cc > 0).then(cs / cc).otherwise(None)


_BASES: list[str] = []


def _reg(name: str, expr: pl.Expr, description: str) -> None:
    """Register a mirrored, NaN-passthrough in-tournament feature and track it."""
    feature(name=name, params=[], description=description, mirror=True, impute=None)(
        lambda _e=expr: _e
    )
    _BASES.append(name)


# --- per-match quantities (player perspective; engine mirrors to opp_*) -------

ONE = pl.lit(1.0)
WON_I = pl.col("won").cast(pl.Int64)
MATCH_IND = pl.col("match_uid").is_not_null().cast(pl.Int64)

# Points
PS_WON, PS_PLAYED = pl.col("pts_service_pts_won"), pl.col("pts_service_pts_played")
PR_WON, PR_PLAYED = pl.col("pts_return_pts_won"), pl.col("pts_return_pts_played")
PT_WON, PT_PLAYED = pl.col("pts_total_pts_won"), pl.col("pts_total_pts_played")

# Games — three-part decomposition (holds / breaks / tiebreaks)
SVC_GP, RET_GP = pl.col("svc_games_played"), pl.col("ret_games_played")
HOLDS = SVC_GP - (pl.col("svc_bp_faced") - pl.col("svc_bp_saved"))
BREAKS = pl.col("ret_bp_converted")
TBW = _tiebreaks_won()
TBP = _tiebreaks_played()
SVC_BROKEN = pl.col("svc_bp_faced") - pl.col("svc_bp_saved")
RET_LOST = RET_GP - pl.col("ret_bp_converted")
TB_LOST = TBP - TBW
TOTAL_GW = HOLDS + BREAKS + TBW
TOTAL_GL = SVC_BROKEN + RET_LOST + TB_LOST
TOTAL_GP = SVC_GP + RET_GP + TBP

# Sets + score character
SW, SL = _sets_won(), _sets_lost()
SP = SW + SL
TIGHT, BLOWOUT, STRAIGHT = _tight_sets(), _blowout_sets(), _is_straight_set_win()
DEC = _deciding_set_flag()


# --- counts ------------------------------------------------------------------

for _name, _expr, _desc in [
    ("tourn_pts_service_won", PS_WON, "Cumulative service points won in tournament"),
    ("tourn_pts_service_lost", PS_PLAYED - PS_WON, "Cumulative service points lost in tournament"),
    ("tourn_pts_service_played", PS_PLAYED, "Cumulative service points played in tournament"),
    ("tourn_pts_return_won", PR_WON, "Cumulative return points won in tournament"),
    ("tourn_pts_return_lost", PR_PLAYED - PR_WON, "Cumulative return points lost in tournament"),
    ("tourn_pts_return_played", PR_PLAYED, "Cumulative return points played in tournament"),
    ("tourn_pts_total_won", PT_WON, "Cumulative total points won in tournament"),
    ("tourn_pts_total_lost", PT_PLAYED - PT_WON, "Cumulative total points lost in tournament"),
    ("tourn_pts_total_played", PT_PLAYED, "Cumulative total points played in tournament"),
    ("tourn_holds", HOLDS, "Cumulative service games held in tournament"),
    ("tourn_breaks", BREAKS, "Cumulative return games won (breaks) in tournament"),
    ("tourn_tiebreaks_won", TBW, "Cumulative tiebreak games won in tournament"),
    ("tourn_svc_games_lost", SVC_BROKEN, "Cumulative service games lost (broken) in tournament"),
    ("tourn_ret_games_lost", RET_LOST, "Cumulative return games lost in tournament"),
    ("tourn_svc_games_played", SVC_GP, "Cumulative service games played in tournament"),
    ("tourn_ret_games_played", RET_GP, "Cumulative return games played in tournament"),
    ("tourn_sets_played", SP, "Cumulative sets played in tournament"),
    ("tourn_tight_sets", TIGHT, "Cumulative tight sets (7-5/7-6) in tournament"),
    ("tourn_blowout_sets", BLOWOUT, "Cumulative blowout sets (6-0/6-1) in tournament"),
    ("tourn_straight_set_wins", STRAIGHT, "Cumulative straight-set wins in tournament"),
    ("tourn_tiebreaks_played", TBP, "Cumulative tiebreaks played in tournament"),
    ("tourn_deciding_sets", DEC, "Cumulative deciding sets reached in tournament"),
]:
    _reg(_name, _cum(_expr), _desc)

# Singles-match workload count: mirror the empty-state behavior of its existing
# sibling `tourn_matches_played` (0-on-first / impute=0), not the result-count
# null convention the rest of this block uses.
feature(
    name="tourn_singles_played", params=[], mirror=True, impute=0,
    description="Cumulative singles matches played in tournament",
)(lambda: MATCH_IND.cum_sum().shift(1).over(TOURN_GROUP, order_by=ORDER).fill_null(0))
_BASES.append("tourn_singles_played")


# --- point rates: won {per played, game, set, match}, lost/played {game, set, match}

for _dom, _won, _played, _gp in [
    ("service", PS_WON, PS_PLAYED, SVC_GP),
    ("return", PR_WON, PR_PLAYED, RET_GP),
    ("total", PT_WON, PT_PLAYED, SVC_GP + RET_GP),
]:
    _lost = _played - _won
    _reg(f"tourn_pts_{_dom}_won_pct", _rate(_won, _played), f"In-tournament {_dom} points won % (per point played)")
    _reg(f"tourn_pts_{_dom}_won_per_game", _rate(_won, _gp), f"In-tournament {_dom} points won per {_dom} game")
    _reg(f"tourn_pts_{_dom}_won_per_set", _rate(_won, SP), f"In-tournament {_dom} points won per set")
    _reg(f"tourn_pts_{_dom}_won_per_match", _rate(_won, ONE), f"In-tournament {_dom} points won per match")
    _reg(f"tourn_pts_{_dom}_lost_per_game", _rate(_lost, _gp), f"In-tournament {_dom} points lost per {_dom} game")
    _reg(f"tourn_pts_{_dom}_lost_per_set", _rate(_lost, SP), f"In-tournament {_dom} points lost per set")
    _reg(f"tourn_pts_{_dom}_lost_per_match", _rate(_lost, ONE), f"In-tournament {_dom} points lost per match")
    _reg(f"tourn_pts_{_dom}_played_per_game", _rate(_played, _gp), f"In-tournament {_dom} points played per {_dom} game")
    _reg(f"tourn_pts_{_dom}_played_per_set", _rate(_played, SP), f"In-tournament {_dom} points played per set")
    _reg(f"tourn_pts_{_dom}_played_per_match", _rate(_played, ONE), f"In-tournament {_dom} points played per match")


# --- game rates --------------------------------------------------------------

_reg("tourn_hold_pct", _rate(HOLDS, SVC_GP), "In-tournament hold % (service games held / played)")
_reg("tourn_break_pct", _rate(BREAKS, RET_GP), "In-tournament break % (return games won / played)")

for _q, _qn in [("holds", HOLDS), ("breaks", BREAKS), ("games_won", TOTAL_GW),
                ("svc_games_lost", SVC_BROKEN), ("ret_games_lost", RET_LOST), ("games_lost", TOTAL_GL),
                ("svc_games_played", SVC_GP), ("ret_games_played", RET_GP), ("games_played", TOTAL_GP)]:
    _reg(f"tourn_{_q}_per_set", _rate(_qn, SP), f"In-tournament {_q.replace('_', ' ')} per set")
    _reg(f"tourn_{_q}_per_match", _rate(_qn, ONE), f"In-tournament {_q.replace('_', ' ')} per match")


# --- set rates ---------------------------------------------------------------

_reg("tourn_sets_won_pct", _rate(SW, SP), "In-tournament sets won % (of sets played)")
_reg("tourn_sets_won_per_match", _rate(SW, ONE), "In-tournament sets won per match")
_reg("tourn_sets_lost_per_match", _rate(SL, ONE), "In-tournament sets lost per match")
_reg("tourn_sets_played_per_match", _rate(SP, ONE), "In-tournament sets played per match")


# --- serve / return sub-stats (efficiency rates + ratings + volume) ----------

for _name, _num, _den in [
    ("tourn_svc_first_serve_win_pct", "svc_first_serve_pts_won", "svc_first_serve_pts_played"),
    ("tourn_svc_second_serve_win_pct", "svc_second_serve_pts_won", "svc_second_serve_pts_played"),
    ("tourn_svc_ace_pct", "svc_aces", "svc_first_serve_att"),
    ("tourn_svc_df_pct", "svc_double_faults", "svc_first_serve_att"),
    ("tourn_svc_first_serve_in_pct", "svc_first_serve_in", "svc_first_serve_att"),
    ("tourn_svc_bp_save_pct", "svc_bp_saved", "svc_bp_faced"),
    ("tourn_ret_first_serve_win_pct", "ret_first_serve_pts_won", "ret_first_serve_pts_played"),
    ("tourn_ret_second_serve_win_pct", "ret_second_serve_pts_won", "ret_second_serve_pts_played"),
    ("tourn_ret_bp_convert_pct", "ret_bp_converted", "ret_bp_opportunities"),
]:
    _reg(_name, _rate(pl.col(_num), pl.col(_den)), f"In-tournament {_name.removeprefix('tourn_')}")

_reg("tourn_svc_rating", _mean("svc_serve_rating"), "In-tournament mean ATP serve rating")
_reg("tourn_ret_rating", _mean("ret_return_rating"), "In-tournament mean ATP return rating")

for _q, _col in [("svc_aces", "svc_aces"), ("svc_df", "svc_double_faults"),
                 ("svc_bp_faced", "svc_bp_faced"), ("ret_bp_opportunities", "ret_bp_opportunities")]:
    _reg(f"tourn_{_q}_per_match", _rate(pl.col(_col), ONE), f"In-tournament {_q.replace('_', ' ')} per match")
    _reg(f"tourn_{_q}_per_set", _rate(pl.col(_col), SP), f"In-tournament {_q.replace('_', ' ')} per set")


# --- score-character rates ---------------------------------------------------

_reg("tourn_tight_set_pct", _rate(TIGHT, SP), "In-tournament tight-set (7-5/7-6) rate per set")
_reg("tourn_blowout_set_pct", _rate(BLOWOUT, SP), "In-tournament blowout-set (6-0/6-1) rate per set")
_reg("tourn_straight_set_win_pct", _rate(STRAIGHT, ONE), "In-tournament straight-set-win rate per match")
_reg("tourn_tiebreak_win_pct", _rate(TBW, TBP), "In-tournament tiebreak win % (of tiebreaks played)")
_reg("tourn_tiebreaks_per_match", _rate(TBP, ONE), "In-tournament tiebreaks played per match")
_reg("tourn_deciding_set_pct", _rate(DEC, ONE), "In-tournament deciding-set rate per match")
_reg("tourn_deciding_set_win_pct", _rate(DEC * WON_I, DEC), "In-tournament deciding-set win % (of deciding sets)")


# --- diffs (player - opp) on every base --------------------------------------

for _b in list(_BASES):
    register_diff(_b)


# --- matchups (efficiency rates only; both directions) -----------------------

for _name, _pc, _oc, _d1, _d2, _desc in [
    ("tourn_svc_pts_won_pct_matchup", "player_tourn_pts_service_won_pct", "opp_tourn_pts_return_won_pct",
     "tourn_pts_service_won_pct", "tourn_pts_return_won_pct", "In-tournament service pts won% minus opp return pts won%"),
    ("tourn_ret_pts_won_pct_matchup", "player_tourn_pts_return_won_pct", "opp_tourn_pts_service_won_pct",
     "tourn_pts_return_won_pct", "tourn_pts_service_won_pct", "In-tournament return pts won% minus opp service pts won%"),
    ("tourn_hold_vs_break_matchup", "player_tourn_hold_pct", "opp_tourn_break_pct",
     "tourn_hold_pct", "tourn_break_pct", "In-tournament hold% minus opp break%"),
    ("tourn_break_vs_hold_matchup", "player_tourn_break_pct", "opp_tourn_hold_pct",
     "tourn_break_pct", "tourn_hold_pct", "In-tournament break% minus opp hold%"),
    ("tourn_svc_first_serve_win_pct_matchup", "player_tourn_svc_first_serve_win_pct", "opp_tourn_ret_first_serve_win_pct",
     "tourn_svc_first_serve_win_pct", "tourn_ret_first_serve_win_pct", "In-tournament 1st-serve win% minus opp 1st-return win%"),
    ("tourn_ret_first_serve_win_pct_matchup", "player_tourn_ret_first_serve_win_pct", "opp_tourn_svc_first_serve_win_pct",
     "tourn_ret_first_serve_win_pct", "tourn_svc_first_serve_win_pct", "In-tournament 1st-return win% minus opp 1st-serve win%"),
    ("tourn_svc_second_serve_win_pct_matchup", "player_tourn_svc_second_serve_win_pct", "opp_tourn_ret_second_serve_win_pct",
     "tourn_svc_second_serve_win_pct", "tourn_ret_second_serve_win_pct", "In-tournament 2nd-serve win% minus opp 2nd-return win%"),
    ("tourn_ret_second_serve_win_pct_matchup", "player_tourn_ret_second_serve_win_pct", "opp_tourn_svc_second_serve_win_pct",
     "tourn_ret_second_serve_win_pct", "tourn_svc_second_serve_win_pct", "In-tournament 2nd-return win% minus opp 2nd-serve win%"),
    ("tourn_svc_bp_pct_matchup", "player_tourn_svc_bp_save_pct", "opp_tourn_ret_bp_convert_pct",
     "tourn_svc_bp_save_pct", "tourn_ret_bp_convert_pct", "In-tournament BP save% minus opp BP convert%"),
    ("tourn_ret_bp_pct_matchup", "player_tourn_ret_bp_convert_pct", "opp_tourn_svc_bp_save_pct",
     "tourn_ret_bp_convert_pct", "tourn_svc_bp_save_pct", "In-tournament BP convert% minus opp BP save%"),
]:
    register_matchup(_name, _pc, _oc, _d1, _d2, _desc)
