"""Tennis projector that wraps a serve model + the IID chain math.

The projector takes a polars DataFrame of matches (one row per match, with
both players' features) and produces a per-match distribution over outcomes
via the standard tennis chain. It is purely orchestration: it pulls per-point
serve win probs from a `ServeWinProbEstimator`, derives per-game hold prob
and per-tiebreak win prob via `chain.p_service_game_win` /
`chain.p_tiebreak_game_win`, and runs `chain.match_distribution`.
"""

import logging
import time
from dataclasses import dataclass

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

from mvp.projection.iid.chain import (
    MatchDistribution,
    match_distribution,
    p_service_game_win,
    p_tiebreak_game_win,
    set_score_distribution,
)
from mvp.projection.iid.serve_model import ServeWinProbEstimator
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn


@dataclass
class ProjectionOutput:
    """Output of `TennisProjector.project`. Aligned by row to the input DataFrame."""

    distribution: MatchDistribution
    match_uid: np.ndarray
    best_of: np.ndarray
    p_a_serve_win: np.ndarray
    p_b_serve_win: np.ndarray
    h_a: np.ndarray
    h_b: np.ndarray
    t_ab: np.ndarray
    # Per-match (N, 14) set-score pmf, column order = chain.SET_SCORE_LABELS.
    #
    # Computed HERE rather than recomputed downstream from `h_a`/`h_b`/`t_ab`.
    # Three consumers used to call `set_score_distribution` on those scalars
    # themselves (the two set-score/tiebreak diagnostics and the predicted-shape
    # bucketing). That is fine while the serve model emits a point estimate and
    # wrong the moment it emits a distribution: the scalar fields are then a
    # reduction over draws, and rebuilding the pmf from a reduced `p` gives the
    # pmf of a representative match rather than the mixture over the posterior —
    # silently, since the shapes still line up. Producing it at the point where
    # the draws exist removes that route instead of relying on each consumer to
    # remember.
    set_score_pmf: np.ndarray


class _DrawAccumulator:
    """Running sum of per-draw chain outputs, averaged into the posterior mixture.

    Why a mean is the right aggregation, field by field: every array on
    `MatchDistribution` is a probability or an expectation *of* the outcome
    distribution, so mixing over the posterior is the per-field mean. The
    derived quantities are all linear in those fields — `expected_games_a` is a
    combination of two expectations, `p_over_total` and `p_a_spread_cover` are
    partial sums of a pmf — so deriving from the mixture equals averaging the
    per-draw derived values, and no consumer has to know draws happened.
    `spread_offset` is a storage constant (`chain.py:371`), identical every draw.

    What is NOT preserved is the identity between the scalar fields. Under more
    than one draw `p_a_serve_win` is E[p] while `h_a` is E[h(p)], and `h` is
    convex, so `p_service_game_win(out.p_a_serve_win) != out.h_a`. Each is the
    correct posterior mean of its own quantity; anything recomputing one from
    another gets a point answer. `set_score_pmf` is likewise E[pmf(p)], the
    mixture — not the pmf of E[p].

    Accumulating rather than collecting keeps memory flat in the draw count: a
    200-draw run over N matches would otherwise hold 200 copies of the (N, 131)
    spread pmf.

    At `n_draws == 1` this is bit-identical to computing the distribution
    directly — `0.0 + x` and `x / 1.0` are exact in IEEE754.
    """

    def __init__(self) -> None:
        self._n = 0
        self._spread_offset: int | None = None
        self._sums: dict[str, np.ndarray] = {}
        self._set_outcomes: dict[tuple[int, int], np.ndarray] = {}

    def _accumulate(self, key: str, value: np.ndarray) -> None:
        current = self._sums.get(key)
        if current is None:
            self._sums[key] = value.astype(np.float64, copy=True)
        else:
            current += value

    def add(
        self,
        dist: MatchDistribution,
        *,
        p_a: np.ndarray,
        p_b: np.ndarray,
        h_a: np.ndarray,
        h_b: np.ndarray,
        t_ab: np.ndarray,
        set_score_pmf: np.ndarray,
    ) -> None:
        if self._spread_offset is None:
            self._spread_offset = dist.spread_offset
        elif self._spread_offset != dist.spread_offset:
            raise ValueError(
                "spread_offset differs across draws "
                f"({self._spread_offset} vs {dist.spread_offset}); the spread "
                "pmfs are on different supports and cannot be averaged"
            )

        self._accumulate("p_match_win_a", dist.p_match_win_a)
        self._accumulate("total_games_pmf", dist.total_games_pmf)
        self._accumulate("spread_pmf", dist.spread_pmf)
        self._accumulate("expected_total_games", dist.expected_total_games)
        self._accumulate("expected_spread", dist.expected_spread)
        self._accumulate("p_a_serve_win", p_a)
        self._accumulate("p_b_serve_win", p_b)
        self._accumulate("h_a", h_a)
        self._accumulate("h_b", h_b)
        self._accumulate("t_ab", t_ab)
        self._accumulate("set_score_pmf", set_score_pmf)

        # Key sets are stable across draws in practice (the same matches, so
        # the same best_of values), but a draw missing a key must contribute
        # zero to it rather than shorten the average.
        for key, vec in dist.set_outcome_probs.items():
            current = self._set_outcomes.get(key)
            if current is None:
                self._set_outcomes[key] = np.zeros_like(vec, dtype=np.float64)
                current = self._set_outcomes[key]
            current += vec

        self._n += 1

    def finalize(
        self, *, match_uid: np.ndarray, best_of: np.ndarray,
    ) -> ProjectionOutput:
        if self._n == 0:
            raise ValueError("No draws accumulated")
        n = float(self._n)
        mean = {key: value / n for key, value in self._sums.items()}
        set_outcomes = {key: v / n for key, v in self._set_outcomes.items()}

        assert self._spread_offset is not None
        dist = MatchDistribution(
            p_match_win_a=mean["p_match_win_a"],
            set_outcome_probs=set_outcomes,
            total_games_pmf=mean["total_games_pmf"],
            spread_pmf=mean["spread_pmf"],
            spread_offset=self._spread_offset,
            expected_total_games=mean["expected_total_games"],
            expected_spread=mean["expected_spread"],
        )
        return ProjectionOutput(
            distribution=dist,
            match_uid=match_uid,
            best_of=best_of,
            p_a_serve_win=mean["p_a_serve_win"],
            p_b_serve_win=mean["p_b_serve_win"],
            h_a=mean["h_a"],
            h_b=mean["h_b"],
            t_ab=mean["t_ab"],
            set_score_pmf=mean["set_score_pmf"],
        )


class TennisProjector:
    """Composes a `ServeWinProbEstimator` with the IID chain math.

    Workflow per call:
        serve_model.predict(df) → (p_a, p_b)
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        match_distribution(h_a, h_b, t_ab, best_of)
    """

    def __init__(self, serve_model: ServeWinProbEstimator) -> None:
        self.serve_model = serve_model

    def fit(self, df: pl.DataFrame) -> None:
        """Fit the underlying serve model. Pass-through."""
        self.serve_model.fit(df)

    def project(
        self,
        df: pl.DataFrame,
        *,
        best_of_col: str = "best_of",
        match_uid_col: str = "match_uid",
    ) -> ProjectionOutput:
        """Project distributions for each row of `df`.

        Each row of `df` must be a single match (one row per match) — collapse
        mirrored player rows BEFORE calling `project`. The runner takes care
        of this collapse.
        """
        if best_of_col not in df.columns:
            raise ValueError(f"DataFrame missing required column: {best_of_col}")
        if match_uid_col not in df.columns:
            raise ValueError(f"DataFrame missing required column: {match_uid_col}")
        for col in self.serve_model.required_columns:
            if col not in df.columns:
                raise ValueError(
                    f"DataFrame missing serve-model column: {col}"
                )

        best_of = df[best_of_col].to_numpy().astype(np.int64)
        match_uid: np.ndarray = df[match_uid_col].to_numpy()

        n_draws = int(self.serve_model.n_draws)
        if n_draws < 1:
            raise ValueError(f"n_draws must be >= 1, got {n_draws}")

        logger.info("Projecting %d matches over %d draw(s)", len(df), n_draws)
        t0 = time.perf_counter()

        acc = _DrawAccumulator()
        for draw in range(n_draws):
            if self.serve_model.is_state_aware:
                # One call, not two. Asking for the state callables and the
                # neutral-state scalars separately made a state-aware model
                # rebuild its whole per-match feature matrix twice per draw —
                # doubling the dominant cost of a Monte-Carlo run — and left
                # the first call's closures reading instance state the second
                # call had reassigned. They agreed only because both calls
                # passed the same `df` and `draw`.
                (
                    p_a_fn, p_b_fn, p_a, p_b,
                ) = self.serve_model.predict_state_fn_and_neutral(df, draw)
                h_a = p_service_game_win(p_a)
                h_b = p_service_game_win(p_b)
                t_ab = p_tiebreak_game_win(p_a, p_b)
                dist = match_distribution_from_state_fn(
                    p_a_fn, p_b_fn, p_a, p_b, best_of,
                )
            else:
                p_a, p_b = self.serve_model.predict_draw(df, draw)
                h_a = p_service_game_win(p_a)
                h_b = p_service_game_win(p_b)
                t_ab = p_tiebreak_game_win(p_a, p_b)
                dist = match_distribution(h_a, h_b, t_ab, best_of)

            acc.add(
                dist,
                p_a=p_a, p_b=p_b, h_a=h_a, h_b=h_b, t_ab=t_ab,
                set_score_pmf=set_score_distribution(h_a, h_b, t_ab),
            )

        logger.info("Projection complete in %.1fs", time.perf_counter() - t0)
        return acc.finalize(match_uid=match_uid, best_of=best_of)
