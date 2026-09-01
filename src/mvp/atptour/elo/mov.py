"""Margin-of-victory Elo variants: bare base ratings updated by HOW a match
was won, not just whether (plan 2026-09-01-mov-elo-ratings, rev 3).

Two variants, each a separate rating with its OWN rd and match count (the
k-factor schedule's inputs), and deliberately NO surface/indoor adjustment
layers — v1 gates the bare mechanism first:

- ``melo``: the update's outcome is the per-player games share
  (own games / total games, clipped), so ``S_A + S_B == 1`` exactly as
  ``E_A + E_B == 1``. A double-bagel moves ratings a lot; a three-tiebreak
  win barely; a loser who wins many games loses little.
- ``kmov``: binary outcome kept, K multiplied by a bounded monotone function
  of the games-share margin. Its population-level unbiasedness is a
  HYPOTHESIS (the drift term is E(1-E)*(E[mult|win]-E[mult|loss])); the
  tracker collects the favorite/underdog diagnostic that tests it, and a
  rating-gap damping term is added iff that diagnostic shows drift.

Incomplete-match guard (both variants): fall back to the standard binary
update whenever total games is zero/missing — the PRIMARY guard, it is what
literally divides by zero, and it catches the ~2.8k result_type-only
walkovers and ~300 unflagged legacy rows that `reason` alone misses (the
completeness.py lesson) — or when reason/result_type flag the match
incomplete: a retirement's margin measures the injury, not the players.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import DEFAULT_ELO, DEFAULT_RD, REVERSION_RATE
from mvp.atptour.elo.ratings import (
    apply_inactivity_rd,
    expected_score,
    k_factor_from,
    update_rd,
)

VARIANTS = ("melo", "kmov", "kflat")

# Keep S off exact 0/1: a 12-0 sweep is dominant, not infinite evidence.
SHARE_CLIP = 0.02
# melo K rescale (diagnostic-derived, 2026-09-01): the binary-tuned K schedule
# assumed |out-E|-scale movement; melo's |S-E| runs ~3.79x smaller (pooled
# |out-E|/|S-E| over 701,515 margin-valid matches). Bins orthogonal to margin
# (favorite-strength terciles 3.44-4.00, circuit 3.62-4.31) are flat within
# ~10%, so one constant; the total-games gradient (2.1-6.5) is the margin
# signal itself and must NOT be flattened. Applied to margin-valid updates
# only — the binary fallback stays at unscaled K, preserving its exact
# equivalence to standard elo.
MELO_K_SCALE = 3.79
# kmov multiplier: 1 + GAIN * (2*|share-0.5|), capped. A 50/50-games match
# updates at K; a shutout at min(1+GAIN, CAP) * K.
KMOV_GAIN = 1.0
KMOV_CAP = 2.0

# ABLATION (review alignment 2026-09-01): kmov's multiplier is >= 1 on every
# real match, so its whole recursion runs hot unconditionally — its clean
# uniform floor gain may be a BASE_K-undertuned finding wearing a MOV costume.
# `kflat` applies a CONSTANT multiplier equal to kmov's measured population
# mean (weighted from the gate diagnostics: (1.3127*482458 + 1.2408*219057) /
# 701515), on exactly the rows kmov boosts (margin-valid; fallback stays 1.0),
# so the ONLY difference between kflat and kmov is the margin-dependence.
# kflat matching kmov's floor falsifies the margin mechanism.
KFLAT_MULT = 1.2903

# STRICTER than completeness.py's default on purpose: that predicate excludes
# only walkovers unless exclude_incomplete opts in RET/DEF/UNP; here all four
# always invalidate the MARGIN (the match still updates, binary) — a
# retirement's margin measures the injury.
_INCOMPLETE_REASONS = frozenset({"W/O", "RET", "DEF", "UNP"})


def games_share(games_won: float, games_lost: float) -> float:
    """Per-player own-games share, clipped into (0,1). Caller guarantees a
    positive total via `margin_is_valid` — this function never sees 0/0."""
    share = games_won / (games_won + games_lost)
    return min(max(share, SHARE_CLIP), 1.0 - SHARE_CLIP)


def kmov_multiplier(share: float) -> float:
    """Bounded, monotone in |share - 0.5|; 1.0 at a games-even match."""
    return min(1.0 + KMOV_GAIN * 2.0 * abs(share - 0.5), KMOV_CAP)


def margin_is_valid(
    total_games: float | int | None,
    reason: str | None,
    result_type: str | None,
) -> bool:
    if not total_games:  # None or 0: the literal 0/0
        return False
    if reason is not None and reason in _INCOMPLETE_REASONS:
        return False
    if result_type == "walkover":
        return False
    return True


@dataclass
class MovState:
    rating: float
    rd: float = DEFAULT_RD
    match_count: int = 0
    last_match_date: date | None = None


class MovTracker:
    """Per-variant per-player state threaded through compute_all_ratings.

    Self-contained on purpose: the driver's seam calls five methods at the
    same points where the existing elo/glicko state is initialized, decayed,
    captured, replayed and updated — no variant logic leaks into the loop
    body. With no tracker passed, the driver's behavior is untouched.

    ONE tracker per compute_all_ratings call: unlike the driver's own
    elo/glicko dicts (fresh per call), this state is caller-owned — reusing
    a tracker across two calls would double-process every match without
    error.
    """

    def __init__(self, variants: tuple[str, ...] = VARIANTS) -> None:
        unknown = set(variants) - set(VARIANTS)
        if unknown:
            raise ValueError(f"unknown MOV variant(s): {sorted(unknown)}")
        self.variants = tuple(variants)
        self._state: dict[str, dict[str, MovState]] = {v: {} for v in self.variants}
        # Diagnostics the plan's gate requires (collected over UPDATE calls,
        # margin-valid rows only): melo's |S-E| scale read; kmov's signed
        # multiplier by favorite/underdog (the drift hypothesis test).
        self.diag_abs_s_minus_e_sum = 0.0
        self.diag_abs_s_minus_e_binary_sum = 0.0
        self.diag_n = 0
        self.diag_mult_favorite_sum = 0.0
        self.diag_n_favorite = 0
        self.diag_mult_underdog_sum = 0.0
        self.diag_n_underdog = 0

    def output_columns(self) -> list[str]:
        return [f"{side}_{v}" for v in self.variants for side in ("player", "opp")]

    def ensure_player(self, player_id: str, seed_elo: float) -> None:
        for v in self.variants:
            self._state[v].setdefault(player_id, MovState(rating=seed_elo))

    def apply_inactivity(self, player_id: str, match_date: date) -> None:
        for v in self.variants:
            st = self._state[v][player_id]
            st.rd = apply_inactivity_rd(st.rd, st.last_match_date, match_date)

    def capture(self, player_id: str) -> dict[str, float]:
        """PRE-match values, cached per match exactly like the elo capture."""
        return {v: self._state[v][player_id].rating for v in self.variants}

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
        won: bool,
        round_name: str,
        tournament_level: str,
        player_games: float,
        opp_games: float,
        margin_valid: bool,
        match_date: date | None,
    ) -> None:
        share_p = (
            games_share(player_games, opp_games) if margin_valid else None
        )
        for v in self.variants:
            sp = self._state[v][player_id]
            so = self._state[v][opp_id]
            k_p = k_factor_from(sp.rd, sp.match_count, round_name, tournament_level)
            k_o = k_factor_from(so.rd, so.match_count, round_name, tournament_level)
            # Both sides use the PRE-update snapshot, like the base rating.
            e_p = expected_score(sp.rating, so.rating)
            e_o = 1.0 - e_p
            out_p = 1.0 if won else 0.0

            if v == "melo":
                s_p = share_p if share_p is not None else out_p
                s_o = 1.0 - s_p
                scale = MELO_K_SCALE if share_p is not None else 1.0
                if share_p is not None:
                    self.diag_abs_s_minus_e_sum += abs(s_p - e_p)
                    self.diag_abs_s_minus_e_binary_sum += abs(out_p - e_p)
                    self.diag_n += 1
                sp.rating += k_p * scale * (s_p - e_p)
                so.rating += k_o * scale * (s_o - e_o)
            elif v == "kmov":
                mult = kmov_multiplier(share_p) if share_p is not None else 1.0
                if share_p is not None:
                    if (e_p >= 0.5) == won:  # favorite won (row-player view)
                        self.diag_mult_favorite_sum += mult
                        self.diag_n_favorite += 1
                    else:
                        self.diag_mult_underdog_sum += mult
                        self.diag_n_underdog += 1
                sp.rating += k_p * mult * (out_p - e_p)
                so.rating += k_o * mult * ((1.0 - out_p) - e_o)
            else:  # kflat — the constant-boost ablation
                mult = KFLAT_MULT if share_p is not None else 1.0
                sp.rating += k_p * mult * (out_p - e_p)
                so.rating += k_o * mult * ((1.0 - out_p) - e_o)

            # Reversion + rd + metadata, mirroring the base rating's block.
            for st in (sp, so):
                reversion = REVERSION_RATE * (st.rd / DEFAULT_RD)
                st.rating += reversion * (DEFAULT_ELO - st.rating)
                st.rd = update_rd(st.rd)
                st.match_count += 1
                if isinstance(match_date, date):
                    st.last_match_date = match_date

    def diagnostics(self) -> dict[str, float | int]:
        out: dict[str, float | int] = {"n_margin_valid": self.diag_n}
        if self.diag_n:
            out["melo_mean_abs_s_minus_e"] = self.diag_abs_s_minus_e_sum / self.diag_n
            out["binary_mean_abs_s_minus_e"] = (
                self.diag_abs_s_minus_e_binary_sum / self.diag_n
            )
        if self.diag_n_favorite:
            out["kmov_mean_mult_favorite_won"] = (
                self.diag_mult_favorite_sum / self.diag_n_favorite
            )
            out["n_favorite_won"] = self.diag_n_favorite
        if self.diag_n_underdog:
            out["kmov_mean_mult_underdog_won"] = (
                self.diag_mult_underdog_sum / self.diag_n_underdog
            )
            out["n_underdog_won"] = self.diag_n_underdog
        return out
