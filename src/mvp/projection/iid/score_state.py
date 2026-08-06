"""`ScoreState` dataclass + helpers for Phase 3 chain integration.

The chain walks a hypothetical point tree for an upcoming match. At each
node, a score-state-aware serve model is evaluated at the CURRENT state
(game score, set score, sets-won, tiebreak flag, etc.). This module provides
the state representation and derivation helpers.

Mirrors the training-time derivations in `score_state_features.py` so that
the flags used at inference match the flags the model was trained on.
"""

from dataclasses import dataclass

# Points won, by displayed game score. The canonical scale for "how far into
# this game are we", shared by the training-time polars derivations
# (`score_state_features.py`) and the inference-time state mapping
# (`serve_model.py`) so the two cannot drift apart.
#
# Points won rather than the displayed 0/15/30/40: the display is an ordinal
# with an arbitrary spacing, and — decisively — it has no meaning in a
# tiebreak, where the score IS a point count. On this scale a tiebreak score
# needs no special case, it is already points won, so one column carries both
# formats without a collision. Deuce is 3-3 and advantage 4-3, which is what
# they are; the (server, returner) PAIR keeps 40-0 and 40-40 distinct even
# though both put the server on 3.
GAME_SCORE_POINTS: dict[str, int] = {
    "0": 0, "15": 1, "30": 2, "40": 3, "D": 3, "AD": 4,
}


@dataclass(frozen=True)
class ScoreState:
    """Pre-point state used by the score-state serve model at inference.

    All fields are derivable from the chain's current position in the DP.
    Frozen to enable use as a dict key / cache key.
    """

    # Game-level
    serve_num: int              # 1 (first serve) or 2 (second serve)
    game_score_server: str      # "0" | "15" | "30" | "40" | "D" | "AD"
    game_score_returner: str
    is_tiebreak: bool

    # Set-level
    set_score_server_games: int
    set_score_returner_games: int

    # Match-level
    sets_won_server: int
    sets_won_returner: int
    best_of: int                # 3 or 5

    def is_break_point(self) -> bool:
        """Returner is one point from winning the server's game.

        Returner is at 40 or AD AND server is below that. In tiebreaks,
        BP doesn't apply (any point is effectively break-territory).
        """
        if self.is_tiebreak:
            return False
        r = self.game_score_returner
        s = self.game_score_server
        if r == "AD":
            return True
        if r == "40" and s in ("0", "15", "30"):
            return True
        return False

    def is_server_game_point(self) -> bool:
        """Server is one point from winning the current game (non-tiebreak)."""
        if self.is_tiebreak:
            return False
        s = self.game_score_server
        r = self.game_score_returner
        if s == "AD":
            return True
        if s == "40" and r in ("0", "15", "30"):
            return True
        return False

    def is_returner_game_point(self) -> bool:
        """Alias for is_break_point — returner winning the game = breaking serve."""
        return self.is_break_point()

    def _server_wins_set_on_this_game(self) -> bool:
        """If server wins the current game, does that win the set?"""
        new_s = self.set_score_server_games + 1
        new_r = self.set_score_returner_games
        # Regular set: 6+ games with 2+ margin.
        if new_s >= 6 and (new_s - new_r) >= 2:
            return True
        # 7-5 win (also caught by above) or 7-game terminus via tiebreak outcome.
        if new_s == 7:
            return True
        return False

    def _returner_wins_set_on_this_game(self) -> bool:
        new_r = self.set_score_returner_games + 1
        new_s = self.set_score_server_games
        if new_r >= 6 and (new_r - new_s) >= 2:
            return True
        if new_r == 7:
            return True
        return False

    def is_set_point(self) -> bool:
        """Either player could win the set by winning this point (non-tiebreak)."""
        if self.is_tiebreak:
            # Tiebreak set-point derivation requires tiebreak-point state,
            # which we don't thread through the ScoreState today. Mirrors the
            # training-time convention of suppressing the flag in tiebreaks.
            return False
        return (
            (self.is_server_game_point() and self._server_wins_set_on_this_game())
            or (self.is_returner_game_point() and self._returner_wins_set_on_this_game())
        )

    def is_server_set_point(self) -> bool:
        if self.is_tiebreak:
            return False
        return self.is_server_game_point() and self._server_wins_set_on_this_game()

    def is_returner_set_point(self) -> bool:
        if self.is_tiebreak:
            return False
        return self.is_returner_game_point() and self._returner_wins_set_on_this_game()

    def _sets_to_win(self) -> int:
        return 3 if self.best_of == 5 else 2

    def is_match_point(self) -> bool:
        if self.is_tiebreak:
            return False
        sets_to_win = self._sets_to_win()
        server_mp = (
            self.is_server_game_point()
            and self._server_wins_set_on_this_game()
            and (self.sets_won_server + 1) >= sets_to_win
        )
        returner_mp = (
            self.is_returner_game_point()
            and self._returner_wins_set_on_this_game()
            and (self.sets_won_returner + 1) >= sets_to_win
        )
        return server_mp or returner_mp

    def is_server_match_point(self) -> bool:
        if self.is_tiebreak:
            return False
        return (
            self.is_server_game_point()
            and self._server_wins_set_on_this_game()
            and (self.sets_won_server + 1) >= self._sets_to_win()
        )

    def is_returner_match_point(self) -> bool:
        if self.is_tiebreak:
            return False
        return (
            self.is_returner_game_point()
            and self._returner_wins_set_on_this_game()
            and (self.sets_won_returner + 1) >= self._sets_to_win()
        )

    def game_points_server(self) -> int | None:
        """Points won by the server in the current game, tiebreak or not.

        `game_score_server` holds "0"/"15"/"30"/"40"/"D"/"AD" in a regular game
        and a raw point count ("0".."22") in a tiebreak — the same field, two
        vocabularies. Returns None for an unrecognized label rather than
        defaulting to 0, because "the score is love" and "the score could not
        be read" are different claims and only one of them is a real state.
        """
        return self._points(self.game_score_server)

    def game_points_returner(self) -> int | None:
        return self._points(self.game_score_returner)

    def _points(self, label: str) -> int | None:
        if self.is_tiebreak:
            try:
                return int(label)
            except (TypeError, ValueError):
                return None
        return GAME_SCORE_POINTS.get(label)

    def game_points_diff(self) -> int | None:
        s, r = self.game_points_server(), self.game_points_returner()
        return None if s is None or r is None else s - r

    def tiebreak_point_diff(self) -> int:
        """Server's tiebreak lead, 0 outside a tiebreak.

        Zero off-tiebreak so a linear model can carry a tiebreak-specific
        slope: the coefficient acts only where the feature is non-zero, which
        is the interaction a linear model cannot otherwise form.
        """
        if not self.is_tiebreak:
            return 0
        d = self.game_points_diff()
        return 0 if d is None else d

    def tiebreak_points_played(self) -> int:
        """Points completed in the current tiebreak, 0 outside one."""
        if not self.is_tiebreak:
            return 0
        s, r = self.game_points_server(), self.game_points_returner()
        return 0 if s is None or r is None else s + r

    def set_score_asymmetry(self) -> int:
        return self.set_score_server_games - self.set_score_returner_games

    def sets_won_asymmetry(self) -> int:
        return self.sets_won_server - self.sets_won_returner


# The 16 non-terminal game-score states (pre-point) in standard scoring.
# (Terminal states like "GAME" are resolved before they're reached in the DP.)
# Deuce and advantage are collapsed: after any "D" both 40-40 and the pre-AD
# deuce return state, the game either ends or returns to deuce — our chain
# treats "D" as a single node that transitions to "AD-40", "40-AD", or stays
# at "D" (handled by p_service_game_win logic).
GAME_SCORE_STATES: tuple[tuple[str, str], ...] = (
    ("0", "0"),
    ("15", "0"), ("0", "15"),
    ("30", "0"), ("15", "15"), ("0", "30"),
    ("40", "0"), ("30", "15"), ("15", "30"), ("0", "40"),
    ("40", "15"), ("30", "30"), ("15", "40"),
    ("40", "30"), ("30", "40"),
    ("D", "D"),
    ("AD", "40"), ("40", "AD"),
)
