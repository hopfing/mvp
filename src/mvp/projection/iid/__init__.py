"""IID/Markov tennis projection package.

Public API:
    chain: pure tennis math (game, tiebreak, set, match distributions)
    projector: TennisProjector — composes a serve model with the chain
    serve_model: ServeWinProbEstimator + IdentityServeModel
"""

from mvp.projection.iid.chain import (
    SET_SCORE_LABELS,
    MatchDistribution,
    match_distribution,
    p_service_game_win,
    p_set_win,
    p_tiebreak_game_win,
    set_score_distribution,
)
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import (
    LEAGUE_MEAN_SERVE_PROB,
    SERVE_PROB_MAX,
    SERVE_PROB_MIN,
    IdentityServeModel,
    MatchupServeModel,
    ServeWinProbEstimator,
)

__all__ = [
    "SET_SCORE_LABELS",
    "MatchDistribution",
    "match_distribution",
    "p_service_game_win",
    "p_set_win",
    "p_tiebreak_game_win",
    "set_score_distribution",
    "ProjectionOutput",
    "TennisProjector",
    "LEAGUE_MEAN_SERVE_PROB",
    "SERVE_PROB_MAX",
    "SERVE_PROB_MIN",
    "IdentityServeModel",
    "MatchupServeModel",
    "ServeWinProbEstimator",
]
