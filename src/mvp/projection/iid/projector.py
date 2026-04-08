"""Tennis projector that wraps a serve model + the IID chain math.

The projector takes a polars DataFrame of matches (one row per match, with
both players' features) and produces a per-match distribution over outcomes
via the standard tennis chain. It is purely orchestration: it pulls per-point
serve win probs from a `ServeWinProbEstimator`, derives per-game hold prob
and per-tiebreak win prob via `chain.p_service_game_win` /
`chain.p_tiebreak_game_win`, and runs `chain.match_distribution`.
"""

from dataclasses import dataclass

import numpy as np
import polars as pl

from mvp.projection.iid.chain import (
    MatchDistribution,
    match_distribution,
    p_service_game_win,
    p_tiebreak_game_win,
)
from mvp.projection.iid.serve_model import ServeWinProbEstimator


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

        p_a, p_b = self.serve_model.predict(df)
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)

        best_of = df[best_of_col].to_numpy().astype(np.int64)
        match_uid: np.ndarray = df[match_uid_col].to_numpy()

        dist = match_distribution(h_a, h_b, t_ab, best_of)

        return ProjectionOutput(
            distribution=dist,
            match_uid=match_uid,
            best_of=best_of,
            p_a_serve_win=p_a,
            p_b_serve_win=p_b,
            h_a=h_a,
            h_b=h_b,
            t_ab=t_ab,
        )
