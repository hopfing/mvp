"""Hand-authored, rule-based ("non-model") win-probability predictor.

A lightweight alternative to the learned models (XGBoost / logistic). Instead
of fitting weights, the user authors a set of *flags* in the model config —
each flag is a registered feature plus an optional pivot, deadband, and a
minimum sample-size guard. Each flag votes -1 / 0 / +1 (favor opponent /
abstain / favor player) by which side of ``pivot`` the value lands on:

    * value >  pivot + deadband  -> +1 (favor player)
    * value <  pivot - deadband  -> -1 (favor opponent)
    * |value - pivot| <= deadband -> 0 (too close to call)
    * value is NaN               -> 0 (one side lacks history; abstain)
    * gated side(s) under min_matches -> 0 (sample too thin to trust)

``pivot`` is the neutral point: 0 for a diff feature (player - opp), 0.5 for a
per-player rate like ``player_h2h_win_pct`` (where "player leads the matchup"
is win% > 0.5). Head-to-head is already player-vs-opp, so its flag uses the
per-player win% with pivot 0.5 — a win% *diff* would be degenerate (the two
sides are complementary).

The flags' net signed vote (sum over flags, an integer in [-k, k] for k flags)
is mapped to a win probability via an empirical, shrinkage-regularized lookup
fit on the training fold. The pipeline's normal calibrator then sits on top,
exactly as for any other model.

The vote->prob map is an artifact of ``fit()`` and is rebuilt on every training
fold, so there is no cross-fold leakage.

Flag spec (``model.params.flags`` entries)::

    - feature: player_win_pct_diff(days=90)   # feature, exactly as in features.include
      pivot: 0.0                              # optional neutral point, default 0.0
      deadband: 0.05                          # optional, default 0.0
      min_matches: 5                          # optional sample-size guard
      count_feature: matches_played           # required iff min_matches set; mirrored base name
      side: both                              # which count to gate: player | opp | both (default both)

The count columns a min_matches guard needs (``player_<count>_<Nd>`` and
``opp_<count>_<Nd>``, using the flag feature's own ``days``) must be present in
``features.include`` so they reach the feature matrix.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from mvp.model.engine import build_column_name, parse_feature_spec
from mvp.model.models import BaseModel


def resolve_flags(
    flags_spec: list[dict[str, Any]], feature_names: list[str]
) -> list[dict[str, Any]]:
    """Map each flag's feature/count specs to column indices in the matrix.

    Shared by RuleBasedModel and the rules evaluator. Raises with a clear
    message if a referenced column is absent from the feature set (the most
    likely config mistake).
    """
    index = {name: i for i, name in enumerate(feature_names)}
    resolved: list[dict[str, Any]] = []
    for f in flags_spec:
        spec = f["feature"]
        _prefix, _base, full_name, params = parse_feature_spec(spec)
        col = build_column_name(full_name, params)
        if col not in index:
            raise ValueError(
                f"flag feature {spec!r} -> column {col!r} is not in the feature "
                f"set; add it to features.include"
            )
        entry: dict[str, Any] = {
            "label": col,
            "col": index[col],
            "pivot": float(f.get("pivot", 0.0)),
            "deadband": float(f.get("deadband", 0.0)),
            "min_matches": f.get("min_matches"),
            "side": f.get("side", "both"),
            "p_count": None,
            "o_count": None,
        }
        if entry["min_matches"] is not None:
            side = entry["side"]
            if side not in ("player", "opp", "both"):
                raise ValueError(
                    f"flag {spec!r}: side must be 'player', 'opp', or 'both', "
                    f"got {side!r}"
                )
            count_feature = f.get("count_feature")
            if not count_feature:
                raise ValueError(
                    f"flag {spec!r} sets min_matches but no count_feature"
                )
            wanted = []
            if side in ("player", "both"):
                wanted.append(("p_count", f"player_{count_feature}"))
            if side in ("opp", "both"):
                wanted.append(("o_count", f"opp_{count_feature}"))
            for key, base in wanted:
                c = build_column_name(base, params)
                if c not in index:
                    raise ValueError(
                        f"flag {spec!r} min_matches needs column {c!r} in "
                        f"features.include"
                    )
                entry[key] = index[c]
        resolved.append(entry)
    return resolved


def flag_vote_matrix(X: np.ndarray, resolved: list[dict[str, Any]]) -> np.ndarray:
    """Per-flag votes, shape (n_rows, n_flags), each in {-1, 0, +1}.

    +1 / -1 = value on the favor-player / favor-opp side of pivot +/- deadband;
    0 = inside the deadband, NaN (one side lacks history), or a thin-sample
    abstention (either side under min_matches).
    """
    votes = np.zeros((X.shape[0], len(resolved)), dtype=np.int64)
    for j, r in enumerate(resolved):
        val = X[:, r["col"]]
        hi = r["pivot"] + r["deadband"]
        lo = r["pivot"] - r["deadband"]
        # NaN comparisons are False on both sides -> falls through to 0.
        v = np.where(val > hi, 1, np.where(val < lo, -1, 0)).astype(np.int64)
        if r["min_matches"] is not None:
            m = r["min_matches"]
            thin = np.zeros(X.shape[0], dtype=bool)
            if r["p_count"] is not None:
                pc = X[:, r["p_count"]]
                thin |= (pc < m) | np.isnan(pc)
            if r["o_count"] is not None:
                oc = X[:, r["o_count"]]
                thin |= (oc < m) | np.isnan(oc)
            v = np.where(thin, 0, v)
        votes[:, j] = v
    return votes


def net_votes(X: np.ndarray, resolved: list[dict[str, Any]]) -> np.ndarray:
    """Net signed vote per row (sum over flags). NaN / thin rows abstain (0)."""
    return flag_vote_matrix(X, resolved).sum(axis=1).astype(np.int64)


class RuleBasedModel(BaseModel):
    """Empirical vote-count predictor over hand-authored flags.

    Parameters (``model.params``):
        flags: list of flag specs (see module docstring). Required, non-empty.
        combine: vote-combination strategy. Only ``"vote_count"`` is supported.
        prior_strength: Bayesian pseudo-count shrinking each vote bucket's
            empirical win rate toward the global base rate (default 15). Guards
            thin extreme buckets (net vote = +/-k with few training rows).
    """

    def __init__(
        self, params: dict[str, Any] | None, feature_names: list[str] | None = None
    ) -> None:
        params = params or {}
        self.flags_spec: list[dict[str, Any]] = params.get("flags", [])
        if not self.flags_spec:
            raise ValueError("RuleBasedModel requires params.flags (a non-empty list)")
        self.combine = params.get("combine", "vote_count")
        if self.combine != "vote_count":
            raise ValueError(
                f"Unsupported combine strategy: {self.combine!r} (only 'vote_count')"
            )
        self.prior_strength = float(params.get("prior_strength", 15.0))
        self.feature_names = list(feature_names) if feature_names is not None else None
        # Fitted state (per fold):
        self._resolved: list[dict[str, Any]] | None = None
        self._vote_to_prob: dict[int, float] | None = None
        self._global_rate: float | None = None

    def _resolve_flags(self, feature_names: list[str]) -> list[dict[str, Any]]:
        return resolve_flags(self.flags_spec, feature_names)

    def _net_votes(
        self, X: np.ndarray, resolved: list[dict[str, Any]]
    ) -> np.ndarray:
        return net_votes(X, resolved)

    def fit(
        self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None
    ) -> None:
        # sample_weight is accepted for interface compatibility; a hand-authored
        # baseline maps raw empirical frequencies, so weights are not applied.
        if self.feature_names is None:
            raise RuntimeError(
                "RuleBasedModel requires feature_names (pass via get_model)"
            )
        self._resolved = self._resolve_flags(self.feature_names)
        y = np.asarray(y, dtype=np.float64)
        net = self._net_votes(X, self._resolved)
        self._global_rate = float(np.clip(y.mean(), 1e-6, 1.0 - 1e-6))
        table: dict[int, float] = {}
        for b in np.unique(net):
            mask = net == b
            n_b = int(mask.sum())
            wins = float(y[mask].sum())
            rate = (wins + self.prior_strength * self._global_rate) / (
                n_b + self.prior_strength
            )
            table[int(b)] = float(np.clip(rate, 1e-6, 1.0 - 1e-6))
        self._vote_to_prob = table

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._vote_to_prob is None or self._resolved is None:
            raise RuntimeError("Model not fitted")
        net = self._net_votes(X, self._resolved)
        # Vote buckets unseen in training fall back to the global base rate.
        out = np.full(X.shape[0], self._global_rate, dtype=np.float64)
        for b, p in self._vote_to_prob.items():
            out[net == b] = p
        return out
