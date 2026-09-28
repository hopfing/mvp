"""Coherent surface-indoor Elo on two outcomes (plan 2026-09-28-surface-indoor-mov-elo).

Two variants, each a base rating plus hard / clay / grass / indoor-hard
adjustments, threaded through compute_all_ratings beside MovTracker:

- ``celo_si``: the binary outcome.
- ``melo_si``: the games-share outcome of mElo (mov.py), with its K rescale on
  margin-valid rows and the binary fallback elsewhere.

Built coherently where the champion ``elo_surface_indoor`` is not
(compute.py's base/surface/indoor block):

- ONE expectation per match, from each player's full effective rating (base
  plus every adjustment the conditions engage); the surprise lands on the base
  and on each engaged adjustment at its own step size, as the serve/return
  axes do. The champion's base and surface updates expect without the indoor
  adjustment, so an indoor specialist's expected indoor wins still move the
  ratings used outdoors.
- Indoor engages on indoor HARD only (`match_axes`), as on the serve side.
- Each adjustment keeps its own rd and clocks and reverts only on matches that
  engage it. The axis rd drives reversion only: it is never emitted and never
  feeds K (adjustments step off the base K, as the serve axes do). Inactivity
  grows it incrementally from the last date it was brought current, so it does
  not compound across the matches in between.

Only the pre-match effective rating for the row's own conditions is emitted,
per variant and side. Rows with no result (``won`` None) capture and emit as
usual but do not update. ``melo_si`` also treats ``result_type == "retirement"``
as margin-invalid (``melo`` itself is unchanged).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    DEFAULT_RD,
    INDOOR_K_MULT,
    REVERSION_RATE,
    SURFACE_K_MULT,
)
from mvp.atptour.elo.mov import MELO_K_SCALE, games_share
from mvp.atptour.elo.ratings import (
    apply_inactivity_rd,
    expected_score,
    k_factor_from,
    match_axes,
    update_rd,
)

COMPOSITE_VARIANTS = ("celo_si", "melo_si")
COMPOSITE_AXES = ("hard", "clay", "grass", "indoor")


@dataclass
class AxisState:
    adj: float = 0.0
    rd: float = DEFAULT_RD
    match_count: int = 0
    last_date: date | None = None
    # The date this axis's rd was last brought current by inactivity or a
    # training match; growth runs from here, so it never compounds.
    rd_as_of: date | None = None


@dataclass
class CompositeState:
    rating: float
    rd: float = DEFAULT_RD
    match_count: int = 0
    last_match_date: date | None = None
    axes: dict[str, AxisState] = field(
        default_factory=lambda: {a: AxisState() for a in COMPOSITE_AXES}
    )

    def effective(self, axes: tuple[str, ...]) -> float:
        return self.rating + sum(self.axes[a].adj for a in axes)


class CompositeEloTracker:
    """Per-variant per-player state threaded through compute_all_ratings.

    The same seam as MovTracker. ONE tracker per compute_all_ratings call:
    the state is caller-owned, so reusing a tracker across two calls would
    double-process every match without error.
    """

    def __init__(self, variants: tuple[str, ...] = COMPOSITE_VARIANTS) -> None:
        unknown = set(variants) - set(COMPOSITE_VARIANTS)
        if unknown:
            raise ValueError(f"unknown composite variant(s): {sorted(unknown)}")
        self.variants = tuple(variants)
        self._state: dict[str, dict[str, CompositeState]] = {v: {} for v in self.variants}

    def output_columns(self) -> list[str]:
        return [f"{side}_{v}" for v in self.variants for side in ("player", "opp")]

    def ensure_player(self, player_id: str, seed_elo: float) -> None:
        for v in self.variants:
            self._state[v].setdefault(player_id, CompositeState(rating=seed_elo))

    def apply_inactivity(self, player_id: str, match_date: date) -> None:
        for v in self.variants:
            st = self._state[v][player_id]
            st.rd = apply_inactivity_rd(st.rd, st.last_match_date, match_date)
            for ax in st.axes.values():
                since = ax.rd_as_of if ax.rd_as_of is not None else ax.last_date
                if since is None:
                    continue
                ax.rd = apply_inactivity_rd(ax.rd, since, match_date)
                ax.rd_as_of = match_date

    def capture(
        self, player_id: str, surface: str | None, indoor: bool | None,
    ) -> dict[str, float]:
        """PRE-match effective rating for this row's conditions, per variant."""
        axes = match_axes(surface, indoor)
        return {v: self._state[v][player_id].effective(axes) for v in self.variants}

    def append_output(
        self,
        output: dict[str, list],
        player_vals: dict[str, float],
        opp_vals: dict[str, float],
    ) -> None:
        for v in self.variants:
            output[f"player_{v}"].append(player_vals[v])
            output[f"opp_{v}"].append(opp_vals[v])

    def update_match(
        self,
        player_id: str,
        opp_id: str,
        won: bool | None,
        round_name: str,
        tournament_level: str,
        surface: str | None,
        indoor: bool | None,
        player_games: float,
        opp_games: float,
        margin_valid: bool,
        result_type: str | None,
        match_date: date | None,
    ) -> None:
        if won is None:
            # No update, but apply_inactivity has already grown the base rd up
            # to this date, so the base clock moves here too; otherwise the next
            # match would count those days again. (Axes keep their own rd_as_of.)
            if isinstance(match_date, date):
                for v in self.variants:
                    for pid in (player_id, opp_id):
                        self._state[v][pid].last_match_date = match_date
            return
        axes = match_axes(surface, indoor)
        out_p = 1.0 if won else 0.0
        melo_valid = margin_valid and result_type != "retirement"
        share_p = games_share(player_games, opp_games) if melo_valid else None
        dated = isinstance(match_date, date)
        for v in self.variants:
            sp = self._state[v][player_id]
            so = self._state[v][opp_id]
            # One expectation, from both players' pre-match effective ratings.
            e_p = expected_score(sp.effective(axes), so.effective(axes))
            e_o = 1.0 - e_p
            if v == "melo_si" and share_p is not None:
                s_p, scale = share_p, MELO_K_SCALE
            else:
                s_p, scale = out_p, 1.0
            s_o = 1.0 - s_p
            k_p = k_factor_from(sp.rd, sp.match_count, round_name, tournament_level) * scale
            k_o = k_factor_from(so.rd, so.match_count, round_name, tournament_level) * scale

            for st, k, s, e in ((sp, k_p, s_p, e_p), (so, k_o, s_o, e_o)):
                surprise = s - e
                st.rating += k * surprise
                for a in axes:
                    mult = INDOOR_K_MULT if a == "indoor" else SURFACE_K_MULT
                    st.axes[a].adj += k * mult * surprise

            # Reversion + rd + metadata: the base every match, each axis only
            # when this match engaged it, by its own rd.
            for st in (sp, so):
                rev = REVERSION_RATE * (st.rd / DEFAULT_RD)
                st.rating += rev * (DEFAULT_ELO - st.rating)
                st.rd = update_rd(st.rd)
                st.match_count += 1
                if dated:
                    st.last_match_date = match_date
                for a in axes:
                    ax = st.axes[a]
                    ax_rev = REVERSION_RATE * (ax.rd / DEFAULT_RD)
                    ax.adj *= 1.0 - ax_rev
                    ax.rd = update_rd(ax.rd)
                    ax.match_count += 1
                    if dated:
                        ax.last_date = match_date
                        ax.rd_as_of = match_date
