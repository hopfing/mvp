"""Score-state-dependent serve model: P(point_won_by_server | features).

Operates at point grain (one row per point). Features mix:
  - match-level (broadcast to every point in a match, server perspective)
  - point-level (vary per point: score state, serve_num, flags)

The output is a calibrated per-point probability. Chain integration (feeding
this into `p_service_game_win` as a score-state-aware callable) happens in
`ScoreStateChainServeModel` (see `serve_model.py`); this module is the pure
point-grain classifier.
"""

from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier


class ScoreStateServeModel(ABC):
    """Point-grain serve-win classifier. Trains/predicts on feature matrices.

    Stores `match_feature_names` and `point_feature_names` separately so the
    chain-inference wrapper can route columns correctly between match-level
    DataFrame lookups and ScoreState-derived values.
    """

    feature_names: list[str]
    match_feature_names: list[str]
    point_feature_names: list[str]

    @abstractmethod
    def fit(
        self, X: np.ndarray, y: np.ndarray, *, groups: np.ndarray | None = None,
    ) -> None:
        """Fit on the point-grain matrix.

        `groups` is the server id per row, supplied for models with a
        per-player parameter. Models without one ignore it.
        """

    @abstractmethod
    def predict_proba(
        self, X: np.ndarray, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        """Return P(point_won_by_server) per row (1-D float64, in [0, 1])."""

    @abstractmethod
    def coef_summary(self) -> dict[str, Any] | None:
        """Per-feature coefficient / importance summary for interpretable forms.

        Returns None for models without natively-interpretable coefficients.
        """


class LogisticScoreStateModel(ScoreStateServeModel):
    """Logistic regression on standardized features.

    Standardization handled inside fit/predict so that NaN → mean imputation
    and std-normalization follow the same contract as MatchupServeModel.
    """

    def __init__(
        self,
        feature_names: list[str],
        params: dict[str, Any] | None = None,
        *,
        match_feature_names: list[str] | None = None,
        point_feature_names: list[str] | None = None,
    ) -> None:
        if not feature_names:
            raise ValueError("feature_names must be non-empty")
        self.feature_names = list(feature_names)
        self.match_feature_names = list(match_feature_names) if match_feature_names is not None else []
        self.point_feature_names = list(point_feature_names) if point_feature_names is not None else []
        # sklearn defaults work reasonably; allow override via config.
        base_params: dict[str, Any] = {
            "max_iter": 1000,
            "solver": "lbfgs",
            "C": 1.0,
        }
        base_params.update(params or {})
        self._params = base_params
        self._model: LogisticRegression | None = None
        self._mean: np.ndarray | None = None
        self._std: np.ndarray | None = None

    def fit(
        self, X: np.ndarray, y: np.ndarray, *, groups: np.ndarray | None = None,
    ) -> None:
        if X.shape[1] != len(self.feature_names):
            raise ValueError(
                f"X has {X.shape[1]} columns but feature_names has {len(self.feature_names)}"
            )
        valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
        X_valid = X[valid].astype(np.float64)
        y_valid = y[valid].astype(np.int64)
        if len(X_valid) == 0:
            raise ValueError("no valid training rows after dropping NaN/non-finite")

        self._mean = X_valid.mean(axis=0)
        self._std = X_valid.std(axis=0)
        self._std = np.where(self._std == 0, 1.0, self._std)
        X_scaled = (X_valid - self._mean) / self._std

        self._model = LogisticRegression(**self._params)
        self._model.fit(X_scaled, y_valid)

    def predict_proba(
        self, X: np.ndarray, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        if self._model is None or self._mean is None or self._std is None:
            raise RuntimeError("LogisticScoreStateModel.predict_proba called before fit")
        X_f = X.astype(np.float64)
        X_f = np.where(np.isnan(X_f), self._mean, X_f)
        X_scaled = (X_f - self._mean) / self._std
        # predict_proba returns shape (N, 2); pick class-1 (point won by server).
        return self._model.predict_proba(X_scaled)[:, 1]

    def coef_summary(self) -> dict[str, Any] | None:
        if self._model is None:
            return None
        coefs = self._model.coef_.ravel()
        intercept = float(self._model.intercept_[0])
        return {
            "intercept": intercept,
            "coefs": dict(zip(self.feature_names, [float(c) for c in coefs], strict=True)),
        }


class BayesianLogisticScoreStateModel(LogisticScoreStateModel):
    """Logistic serve model that emits a POSTERIOR over `p`, not a point.

    The point version already fits a MAP estimate: sklearn's
    `LogisticRegression` with L2 penalty `C` is the mode of a posterior with a
    Gaussian prior `N(0, C)` on the coefficients. All that is missing is the
    curvature around that mode, so this subclass adds the Laplace
    approximation

        posterior(w) ~= N(w_MAP, H^-1),  H = X^T W X + prior_precision

    with `W = diag(p_i (1 - p_i))` evaluated at the MAP fit. Drawing `w` from
    that Gaussian and pushing it through the sigmoid gives a posterior over
    the per-point serve probability.

    Two properties the parameter layer is supposed to have fall out of the
    method rather than being imposed:

      * **Width tracks data.** `X^T W X` grows with the number of informative
        rows, so more data means a tighter posterior. Nothing has to be told
        how many matches backed an estimate.
      * **Thin data shrinks toward the population.** Where the likelihood is
        weak the prior precision dominates, so the coefficient posterior stays
        near zero and predictions fall back to the population base rate — with
        a wide posterior, not a confident guess.

    The intercept is left unpenalized, matching sklearn, so the population
    base rate itself is not shrunk toward zero.

    Draws are addressed by index and seeded from it, so the same fitted model
    returns the same draw every time — a backtest re-run reproduces exactly.
    """

    def __init__(
        self,
        feature_names: list[str],
        params: dict[str, Any] | None = None,
        *,
        match_feature_names: list[str] | None = None,
        point_feature_names: list[str] | None = None,
        n_draws: int = 200,
        seed: int = 0,
        hessian_chunk_rows: int = 500_000,
    ) -> None:
        super().__init__(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
        )
        if n_draws < 1:
            raise ValueError(f"n_draws must be >= 1, got {n_draws}")
        self.n_draws = n_draws
        self.seed = seed
        # The Hessian is accumulated in row chunks: the point grain runs to
        # millions of rows and materializing X^T W X in one shot would hold a
        # second copy of the design matrix.
        self.hessian_chunk_rows = hessian_chunk_rows
        self._posterior_mean: np.ndarray | None = None
        self._posterior_chol: np.ndarray | None = None

    def fit(
        self, X: np.ndarray, y: np.ndarray, *, groups: np.ndarray | None = None,
    ) -> None:
        super().fit(X, y)
        assert self._model is not None and self._mean is not None
        assert self._std is not None

        valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
        X_scaled = (X[valid].astype(np.float64) - self._mean) / self._std

        n_features = X_scaled.shape[1]
        # Augmented design: [features, 1] so the intercept joins the posterior.
        dim = n_features + 1
        hessian = np.zeros((dim, dim), dtype=np.float64)

        coefs = self._model.coef_.ravel()
        intercept = float(self._model.intercept_[0])

        for start in range(0, len(X_scaled), self.hessian_chunk_rows):
            chunk = X_scaled[start : start + self.hessian_chunk_rows]
            logits = chunk @ coefs + intercept
            p = 1.0 / (1.0 + np.exp(-logits))
            w = p * (1.0 - p)
            aug = np.hstack([chunk, np.ones((len(chunk), 1), dtype=np.float64)])
            hessian += aug.T @ (w[:, None] * aug)

        # sklearn's `C` is inverse regularization strength; the implied
        # Gaussian prior on the coefficients has precision 1/C. The intercept
        # is unpenalized, so its prior precision is 0.
        prior_precision = np.zeros(dim, dtype=np.float64)
        prior_precision[:n_features] = 1.0 / float(self._params["C"])
        hessian[np.diag_indices(dim)] += prior_precision

        self._posterior_mean = np.concatenate([coefs, [intercept]])
        self._posterior_chol = self._covariance_cholesky(hessian)

    @staticmethod
    def _covariance_cholesky(hessian: np.ndarray) -> np.ndarray:
        """Lower-triangular L with L L^T = H^-1, for sampling N(0, H^-1).

        Inverting via Cholesky rather than `np.linalg.inv` keeps the result
        symmetric; a tiny jitter covers the case where near-separable features
        make H numerically indefinite.
        """
        dim = hessian.shape[0]
        jitter = 0.0
        scale = float(np.trace(hessian)) / dim
        for _ in range(8):
            try:
                chol_h = np.linalg.cholesky(
                    hessian + jitter * np.eye(dim, dtype=np.float64)
                )
                break
            except np.linalg.LinAlgError:
                jitter = max(scale, 1.0) * 1e-10 if jitter == 0.0 else jitter * 100.0
        else:
            raise np.linalg.LinAlgError(
                "posterior Hessian is not positive definite even with jitter; "
                "the MAP fit is degenerate (perfectly separable features?)"
            )
        # H = L L^T  =>  H^-1 = L^-T L^-1, so L^-T is a valid factor of H^-1:
        # (L^-T)(L^-T)^T = L^-T L^-1 = H^-1.
        l_inv = np.linalg.solve(chol_h, np.eye(dim, dtype=np.float64))
        return l_inv.T

    def sample_coefficients(self, draw: int) -> np.ndarray:
        """Posterior draw `draw` of the augmented coefficient vector."""
        if self._posterior_mean is None or self._posterior_chol is None:
            raise RuntimeError(
                "BayesianLogisticScoreStateModel.sample_coefficients called before fit"
            )
        if not 0 <= draw < self.n_draws:
            raise ValueError(f"draw {draw} out of range [0, {self.n_draws})")
        # Draw 0 is the posterior MEAN, not a sample. That makes a 1-draw
        # configuration collapse to the MAP prediction, so switching draws on
        # and off does not silently move the central estimate as well as its
        # spread.
        if draw == 0:
            return self._posterior_mean
        rng = np.random.default_rng([self.seed, draw])
        z = rng.standard_normal(len(self._posterior_mean))
        return self._posterior_mean + self._posterior_chol @ z

    def predict_proba_draw(
        self, X: np.ndarray, draw: int, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        """`predict_proba` under posterior draw `draw`.

        `groups` is accepted and ignored: this model's posterior is over
        shared coefficients, so it has no per-player parameter.
        """
        if self._mean is None or self._std is None:
            raise RuntimeError(
                "BayesianLogisticScoreStateModel.predict_proba_draw called before fit"
            )
        X_f = X.astype(np.float64)
        X_f = np.where(np.isnan(X_f), self._mean, X_f)
        X_scaled = (X_f - self._mean) / self._std
        w = self.sample_coefficients(draw)
        logits = X_scaled @ w[:-1] + w[-1]
        return 1.0 / (1.0 + np.exp(-logits))


class XGBoostScoreStateModel(ScoreStateServeModel):
    """XGBoost binary classifier on the raw feature matrix.

    No standardization (XGBoost is scale-invariant). NaN handled natively.
    """

    def __init__(
        self,
        feature_names: list[str],
        params: dict[str, Any] | None = None,
        *,
        match_feature_names: list[str] | None = None,
        point_feature_names: list[str] | None = None,
    ) -> None:
        if not feature_names:
            raise ValueError("feature_names must be non-empty")
        self.feature_names = list(feature_names)
        self.match_feature_names = list(match_feature_names) if match_feature_names is not None else []
        self.point_feature_names = list(point_feature_names) if point_feature_names is not None else []
        base_params: dict[str, Any] = {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "n_jobs": -1,
            "random_state": 42,
            "tree_method": "hist",
        }
        base_params.update(params or {})
        self._params = base_params
        self._model: XGBClassifier | None = None

    def fit(
        self, X: np.ndarray, y: np.ndarray, *, groups: np.ndarray | None = None,
    ) -> None:
        if X.shape[1] != len(self.feature_names):
            raise ValueError(
                f"X has {X.shape[1]} columns but feature_names has {len(self.feature_names)}"
            )
        X_f = X.astype(np.float32)
        y_i = y.astype(np.int64)
        self._model = XGBClassifier(**self._params)
        self._model.fit(X_f, y_i)

    def predict_proba(
        self, X: np.ndarray, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("XGBoostScoreStateModel.predict_proba called before fit")
        return self._model.predict_proba(X.astype(np.float32))[:, 1]

    def coef_summary(self) -> dict[str, Any] | None:
        if self._model is None:
            return None
        importances = self._model.feature_importances_
        return {
            "feature_importances": dict(
                zip(self.feature_names, [float(i) for i in importances], strict=True)
            ),
        }


class HierarchicalBoostedScoreStateModel(ScoreStateServeModel):
    """Boosted centre, hierarchical head — the parameter layer with a posterior
    whose width does not vanish at corpus scale.

        logit(p_ij) = f_xgb(x_ij) + u_j,        u_j ~ N(0, tau^2)

    `f_xgb` is an ordinary XGBoost fit, so the nonlinearities and interactions
    that make boosting the right point model for serve data are kept intact.
    `u_j` is a per-server random effect with a learned population variance.

    Why this and not a distributional booster. NGBoost and conformal methods
    put a distribution on the RESPONSE. For a binary response that distribution
    is Bernoulli, whose only parameter is `p` — there is no dispersion
    parameter, because Bernoulli variance is a deterministic function of the
    mean — so such a model emits a point `p` exactly like XGBClassifier does.
    Even on a continuous response their spread is aleatoric (how variable the
    outcome is), and the Markov chain already models that. What is missing is
    epistemic: uncertainty about the PARAMETER. That is what `u_j` carries.

    Why the width survives where a global-coefficient posterior did not. A
    Laplace posterior over shared coefficients tightens as 1/sqrt(N) over the
    whole corpus, so at millions of points it is negligible. `u_j` is
    identified only by player j's own points — hundreds to a few thousand — so
    its posterior stays wide for exactly the players it should.

    Partial pooling falls out of the same prior. The MAP for a thin player is
    pulled toward 0 (the population level set by the booster) because the
    prior term dominates a short likelihood; a data-rich player's own points
    dominate and the effect is free to move. So this corrects the CENTRE for
    thin slices, not merely the width around it.

    The head is separable: absorbing the intercept into the boosted offset
    leaves `u_j` appearing only in player j's rows, so the joint Newton solve
    decomposes into independent per-player scalar solves and the posterior
    covariance is diagonal. That is what makes this tractable at point grain.

    KNOWN APPROXIMATION — the fit is one-pass, not joint. `f_xgb` is fit
    first, then the effects are fit on its offset. The booster and the effects
    explain overlapping variance, so the true joint fit would alternate
    (backfit XGB on effect-adjusted residuals, refit effects, iterate). One
    pass is close when the booster cannot see player identity — as here, where
    no feature encodes it, so `u_j` is unexplainable variance from the
    booster's side — but it is an approximation and a backfitting loop is the
    upgrade path if the effects turn out to absorb booster signal.

    ORDERING — `_u_mean` / `_u_sd` are indexed by `_group_index`, built from
    `np.unique` over the group labels, i.e. LEXICAL order of the label
    strings ("10" sorts before "2"). Lookups go through the dict so the model
    is self-consistent, but indexing those arrays positionally against any
    external player ordering silently scrambles the mapping.
    """

    def __init__(
        self,
        feature_names: list[str],
        params: dict[str, Any] | None = None,
        *,
        match_feature_names: list[str] | None = None,
        point_feature_names: list[str] | None = None,
        n_draws: int = 200,
        seed: int = 0,
        tau_init: float = 0.25,
        n_em_iters: int = 25,
        n_newton_iters: int = 12,
    ) -> None:
        if not feature_names:
            raise ValueError("feature_names must be non-empty")
        if n_draws < 1:
            raise ValueError(f"n_draws must be >= 1, got {n_draws}")
        self.feature_names = list(feature_names)
        self.match_feature_names = list(match_feature_names or [])
        self.point_feature_names = list(point_feature_names or [])
        self.n_draws = n_draws
        self.seed = seed
        self.tau_init = tau_init
        self.n_em_iters = n_em_iters
        self.n_newton_iters = n_newton_iters

        self._booster = XGBoostScoreStateModel(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
        )
        self._group_index: dict[Any, int] = {}
        self._u_mean: np.ndarray | None = None
        self._u_sd: np.ndarray | None = None
        self._tau: float | None = None
        self._group_counts: np.ndarray | None = None

    # ------------------------------------------------------------------
    # Fitting
    # ------------------------------------------------------------------

    @staticmethod
    def _logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
        p = np.clip(p, eps, 1.0 - eps)
        return np.log(p / (1.0 - p))

    def fit(
        self, X: np.ndarray, y: np.ndarray, *, groups: np.ndarray | None = None,
    ) -> None:
        if groups is None:
            raise ValueError(
                "HierarchicalBoostedScoreStateModel requires `groups` (the "
                "server id per row) — the random effect is per player"
            )
        if len(groups) != len(y):
            raise ValueError(
                f"groups has {len(groups)} rows but y has {len(y)}"
            )

        self._booster.fit(X, y)
        offset = self._logit(self._booster.predict_proba(X))

        codes, counts = self._encode_groups(groups)
        y_f = y.astype(np.float64)
        n_groups = len(self._group_index)
        self._group_counts = counts

        # EM over tau: Newton for u | tau, then the normal-normal variance
        # update tau^2 <- mean(u_j^2 + var_j). Including var_j is what stops
        # tau collapsing — the MAP effects are shrunk, so their spread alone
        # underestimates the population variance.
        tau = float(self.tau_init)
        u = np.zeros(n_groups, dtype=np.float64)
        for _ in range(self.n_em_iters):
            u, precision = self._newton_u(offset, y_f, codes, n_groups, tau, u)
            var = 1.0 / precision
            tau_sq = float(np.mean(u ** 2 + var))
            new_tau = float(np.sqrt(max(tau_sq, 1e-12)))
            if abs(new_tau - tau) < 1e-8:
                tau = new_tau
                break
            tau = new_tau

        u, precision = self._newton_u(offset, y_f, codes, n_groups, tau, u)
        self._u_mean = u
        self._u_sd = 1.0 / np.sqrt(precision)
        self._tau = tau

    def _encode_groups(self, groups: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        uniques, codes = np.unique(np.asarray(groups), return_inverse=True)
        self._group_index = {g: i for i, g in enumerate(uniques.tolist())}
        counts = np.bincount(codes, minlength=len(uniques)).astype(np.float64)
        return codes.astype(np.int64), counts

    def _newton_u(
        self,
        offset: np.ndarray,
        y: np.ndarray,
        codes: np.ndarray,
        n_groups: int,
        tau: float,
        u_init: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Per-player Newton solve for the MAP effects and their precision.

        Separable given the offset, so every player is solved at once with
        segment sums rather than a joint linear system.
        """
        prior_precision = 1.0 / (tau * tau)
        u = u_init.copy()
        precision = np.full(n_groups, prior_precision, dtype=np.float64)
        for _ in range(self.n_newton_iters):
            eta = offset + u[codes]
            p = 1.0 / (1.0 + np.exp(-eta))
            grad = np.bincount(codes, weights=(y - p), minlength=n_groups)
            grad -= prior_precision * u
            w = p * (1.0 - p)
            precision = (
                np.bincount(codes, weights=w, minlength=n_groups) + prior_precision
            )
            step = grad / precision
            u += step
            if float(np.max(np.abs(step))) < 1e-9:
                break
        return u, precision

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def _effect_stats(self, groups: np.ndarray | None, n_rows: int):
        """(mean, sd) of the random effect per row.

        A player never seen in training gets mean 0 and sd `tau`: the
        population level, with the population's full spread. That is the
        correct answer for an unknown player rather than a confident guess,
        and it is the behaviour a point model cannot express at all.
        """
        assert self._u_mean is not None and self._u_sd is not None
        assert self._tau is not None
        if groups is None:
            return (
                np.zeros(n_rows, dtype=np.float64),
                np.full(n_rows, self._tau, dtype=np.float64),
            )
        idx = np.array(
            [self._group_index.get(g, -1) for g in np.asarray(groups).tolist()],
            dtype=np.int64,
        )
        known = idx >= 0
        mean = np.zeros(n_rows, dtype=np.float64)
        sd = np.full(n_rows, self._tau, dtype=np.float64)
        mean[known] = self._u_mean[idx[known]]
        sd[known] = self._u_sd[idx[known]]
        return mean, sd

    def predict_proba(
        self, X: np.ndarray, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        if self._u_mean is None:
            raise RuntimeError(
                "HierarchicalBoostedScoreStateModel.predict_proba called before fit"
            )
        offset = self._logit(self._booster.predict_proba(X))
        mean, _ = self._effect_stats(groups, len(offset))
        return 1.0 / (1.0 + np.exp(-(offset + mean)))

    def predict_proba_draw(
        self, X: np.ndarray, draw: int, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        if self._u_mean is None:
            raise RuntimeError(
                "HierarchicalBoostedScoreStateModel.predict_proba_draw called "
                "before fit"
            )
        if not 0 <= draw < self.n_draws:
            raise ValueError(f"draw {draw} out of range [0, {self.n_draws})")
        offset = self._logit(self._booster.predict_proba(X))
        mean, sd = self._effect_stats(groups, len(offset))
        if draw == 0:
            # Draw 0 is the posterior mean, so enabling draws adds spread
            # without moving the centre.
            return 1.0 / (1.0 + np.exp(-(offset + mean)))
        # Seeded on the draw index AND the player, so a given player's effect
        # is drawn consistently across every row they appear in within a draw
        # (both perspectives, every score state) — otherwise the same player
        # would get a different serve level at different points of one match.
        rng = np.random.default_rng([self.seed, draw])
        n_groups = len(self._group_index)
        z_known = rng.standard_normal(n_groups)
        z_unknown = rng.standard_normal(1)[0]
        if groups is None:
            z = np.full(len(offset), z_unknown, dtype=np.float64)
        else:
            idx = np.array(
                [self._group_index.get(g, -1) for g in np.asarray(groups).tolist()],
                dtype=np.int64,
            )
            z = np.where(idx >= 0, z_known[np.clip(idx, 0, None)], z_unknown)
        return 1.0 / (1.0 + np.exp(-(offset + mean + sd * z)))

    def coef_summary(self) -> dict[str, Any] | None:
        if self._u_mean is None or self._tau is None:
            return None
        assert self._u_sd is not None and self._group_counts is not None
        return {
            "tau": self._tau,
            "n_groups": len(self._group_index),
            "effect_sd_median": float(np.median(self._u_sd)),
            "effect_sd_p10": float(np.percentile(self._u_sd, 10)),
            "effect_sd_p90": float(np.percentile(self._u_sd, 90)),
            "median_rows_per_group": float(np.median(self._group_counts)),
            "booster": self._booster.coef_summary(),
        }


def build_score_state_model(
    *,
    type_: str,
    feature_names: list[str],
    params: dict[str, Any] | None = None,
    match_feature_names: list[str] | None = None,
    point_feature_names: list[str] | None = None,
    n_draws: int = 200,
    seed: int = 0,
) -> ScoreStateServeModel:
    if type_ == "logistic":
        return LogisticScoreStateModel(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
        )
    if type_ == "bayesian_logistic":
        return BayesianLogisticScoreStateModel(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
            n_draws=n_draws,
            seed=seed,
        )
    if type_ == "hierarchical_boosted":
        return HierarchicalBoostedScoreStateModel(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
            n_draws=n_draws,
            seed=seed,
        )
    if type_ == "xgboost":
        return XGBoostScoreStateModel(
            feature_names=feature_names,
            params=params,
            match_feature_names=match_feature_names,
            point_feature_names=point_feature_names,
        )
    raise ValueError(f"unknown score-state model type: {type_}")
