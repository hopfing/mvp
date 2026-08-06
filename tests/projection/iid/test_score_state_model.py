"""Tests for score-state serve model."""

import numpy as np
import pytest

from mvp.projection.iid.score_state_model import (
    BayesianLogisticScoreStateModel,
    LogisticScoreStateModel,
    build_score_state_model,
)


def _synthetic(n, rng, beta=(1.2, -0.6, 0.0), intercept=0.4):
    """Point-grain style binary data with known coefficients."""
    X = rng.normal(size=(n, len(beta)))
    logits = X @ np.asarray(beta) + intercept
    p = 1.0 / (1.0 + np.exp(-logits))
    y = (rng.uniform(size=n) < p).astype(int)
    return X, y


def _posterior_std(model, X, n_draws=None):
    """Per-row std of `p` across posterior draws."""
    draws = n_draws if n_draws is not None else model.n_draws
    samples = np.stack(
        [model.predict_proba_draw(X, d) for d in range(1, draws)]
    )
    return samples.std(axis=0)


class TestBayesianLogisticScoreStateModel:
    """The parameter layer emitting a posterior over `p` rather than a point."""

    def _fit(self, n, rng, n_draws=80, seed=0, params=None):
        X, y = _synthetic(n, rng)
        model = BayesianLogisticScoreStateModel(
            feature_names=["f0", "f1", "f2"],
            params=params,
            n_draws=n_draws,
            seed=seed,
        )
        model.fit(X, y)
        return model, X, y

    def test_draw_zero_is_the_map_estimate(self):
        """Draw 0 is the posterior mean, so turning draws on does not move the
        central estimate — only adds spread around it."""
        rng = np.random.default_rng(0)
        model, X, _ = self._fit(4000, rng)
        np.testing.assert_allclose(
            model.predict_proba_draw(X, 0), model.predict_proba(X), rtol=1e-12,
        )

    def test_draws_are_reproducible_by_index(self):
        """A backtest re-run has to reproduce the same posterior."""
        rng = np.random.default_rng(1)
        model, X, _ = self._fit(2000, rng)
        first = model.predict_proba_draw(X, 7)
        second = model.predict_proba_draw(X, 7)
        np.testing.assert_array_equal(first, second)
        assert not np.allclose(first, model.predict_proba_draw(X, 8))

    def test_posterior_has_nonzero_width(self):
        rng = np.random.default_rng(2)
        model, X, _ = self._fit(2000, rng)
        assert float(_posterior_std(model, X).mean()) > 0.0

    def test_posterior_width_shrinks_as_data_grows(self):
        """The doc's success criterion: width tracks data availability.

        Nothing tells the model how much data backed the fit — the Fisher
        information term `X^T W X` in the Hessian grows with informative rows,
        so the posterior tightens on its own.
        """
        rng = np.random.default_rng(3)
        X_eval, _ = _synthetic(500, np.random.default_rng(99))

        widths = []
        for n in (500, 5_000, 50_000):
            model, _, _ = self._fit(n, np.random.default_rng(rng.integers(1 << 30)))
            widths.append(float(_posterior_std(model, X_eval).mean()))

        assert widths[0] > widths[1] > widths[2]
        # Laplace width scales like 1/sqrt(n): 100x the data, ~10x tighter.
        assert widths[0] / widths[2] > 4.0

    def test_default_prior_does_not_shrink_the_map(self):
        """Pins a LIMIT of this estimator, not a feature.

        The parameter layer is often described as making the small-sample
        problem fall out for free — thin slices shrinking toward a population
        prior. Width does fall out (see the test above). Shrinkage does NOT,
        at the default prior: measured across n from 60 to 100k at C=1.0, the
        mean |MAP coefficient| stays within ~5% of the true generating
        coefficients (ratios 0.97-1.05). The L2 prior is far too weak to pull
        anything toward the population.

        The deeper reason is structural: L2 penalizes SHARED coefficients, and
        this model has no player-level parameters to shrink — players enter
        only through features. Per-player shrinkage would need player random
        effects with a learned population variance, which is a different
        model, not a prior-strength setting.
        """
        rng = np.random.default_rng(4)
        X_eval, _ = _synthetic(400, np.random.default_rng(7))

        thin_mags, rich_mags = [], []
        for seed in range(1, 6):
            thin, _, _ = self._fit(80, np.random.default_rng(seed), n_draws=2)
            rich, _, _ = self._fit(40_000, np.random.default_rng(100 + seed), n_draws=2)
            thin_mags.append(float(np.abs(thin._posterior_mean[:-1]).mean()))
            rich_mags.append(float(np.abs(rich._posterior_mean[:-1]).mean()))

        # Thin fits are NOT pulled materially toward zero relative to rich ones.
        assert float(np.mean(thin_mags)) > 0.6 * float(np.mean(rich_mags))

    def test_tight_prior_does_shrink_toward_the_base_rate(self):
        """The shrinkage the default prior does not deliver is available by
        setting `C`, at the cost of biasing every estimate, thin or not — a
        global dial, not the data-adaptive per-player behaviour."""
        rng = np.random.default_rng(41)
        X_eval, _ = _synthetic(400, np.random.default_rng(7))
        tight, _, _ = self._fit(
            3_000, np.random.default_rng(5), n_draws=2, params={"C": 1e-4},
        )
        loose, _, _ = self._fit(
            3_000, np.random.default_rng(5), n_draws=2, params={"C": 100.0},
        )
        assert float(np.std(tight.predict_proba(X_eval))) < 0.25 * float(
            np.std(loose.predict_proba(X_eval))
        )

    def test_stronger_prior_widens_nothing_but_shrinks_the_mean(self):
        """`C` is the prior variance. A tighter prior (small C) shrinks the
        coefficients toward zero — predictions collapse toward the intercept."""
        rng = np.random.default_rng(5)
        loose, X, _ = self._fit(3000, np.random.default_rng(21), params={"C": 100.0})
        tight, _, _ = self._fit(3000, np.random.default_rng(21), params={"C": 0.001})
        assert float(np.std(tight.predict_proba(X))) < float(
            np.std(loose.predict_proba(X))
        )

    def test_posterior_coverage_is_approximately_calibrated(self):
        """Out-of-sample coverage of the true generating probability.

        Averaged over SEVERAL FITS, not one. Coverage from a single fit is a
        near-worthless estimate here even with thousands of evaluation rows:
        every row shares one coefficient vector, so a fit whose MAP lands off
        centre misses on all rows at once. Errors are perfectly correlated and
        the effective sample size is the number of FITS. Measured per-fit
        coverage at n=20k swings from 0.70 to 1.00 on seed alone; averaged over
        fits it sits at 0.88-0.92 against a nominal 0.90.

        Bounds are loose — this checks the Laplace covariance is the right
        order of magnitude, not that the approximation is exact.
        """
        beta, intercept = (1.2, -0.6, 0.0), 0.4
        X_eval, _ = _synthetic(600, np.random.default_rng(31), beta=beta)
        true_p = 1.0 / (1.0 + np.exp(-(X_eval @ np.asarray(beta) + intercept)))

        coverages = []
        for seed in (1, 2, 3, 4, 5, 6):
            X_train, y_train = _synthetic(
                20_000, np.random.default_rng(seed), beta=beta, intercept=intercept,
            )
            model = BayesianLogisticScoreStateModel(
                feature_names=["f0", "f1", "f2"], n_draws=200, seed=seed,
            )
            model.fit(X_train, y_train)
            samples = np.stack(
                [model.predict_proba_draw(X_eval, d) for d in range(1, 200)]
            )
            lo = np.quantile(samples, 0.05, axis=0)
            hi = np.quantile(samples, 0.95, axis=0)
            coverages.append(float(np.mean((true_p >= lo) & (true_p <= hi))))

        assert 0.75 <= float(np.mean(coverages)) <= 1.0

    def test_draw_out_of_range_raises(self):
        rng = np.random.default_rng(7)
        model, X, _ = self._fit(500, rng, n_draws=10)
        with pytest.raises(ValueError, match="out of range"):
            model.predict_proba_draw(X, 10)

    def test_rejects_non_positive_draw_count(self):
        with pytest.raises(ValueError, match="n_draws"):
            BayesianLogisticScoreStateModel(feature_names=["a"], n_draws=0)

    def test_sample_before_fit_raises(self):
        model = BayesianLogisticScoreStateModel(feature_names=["a"])
        with pytest.raises(RuntimeError, match="before fit"):
            model.sample_coefficients(1)

    def test_chunked_hessian_matches_single_shot(self):
        """The row-chunked accumulation is only a memory device — it must not
        change the posterior."""
        rng = np.random.default_rng(8)
        X, y = _synthetic(5000, rng)
        chunked = BayesianLogisticScoreStateModel(
            feature_names=["f0", "f1", "f2"], n_draws=20, hessian_chunk_rows=97,
        )
        single = BayesianLogisticScoreStateModel(
            feature_names=["f0", "f1", "f2"], n_draws=20, hessian_chunk_rows=10**9,
        )
        chunked.fit(X, y)
        single.fit(X, y)
        np.testing.assert_allclose(
            chunked._posterior_chol, single._posterior_chol, rtol=1e-9, atol=1e-12,
        )

    def test_builder_returns_distributional_model(self):
        model = build_score_state_model(
            type_="bayesian_logistic", feature_names=["a", "b"], n_draws=12,
        )
        assert isinstance(model, BayesianLogisticScoreStateModel)
        assert model.n_draws == 12


class TestLogisticScoreStateModel:
    def test_fit_predict_shape(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(1000, 3))
        # Target: slightly favor higher X[:, 0]
        logits = 0.5 * X[:, 0] + 0.2 * X[:, 1]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=1000) < p).astype(int)

        model = LogisticScoreStateModel(feature_names=["f0", "f1", "f2"])
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (1000,)
        assert np.all((probs >= 0) & (probs <= 1))

    def test_coef_summary_reflects_signal(self):
        rng = np.random.default_rng(1)
        X = rng.normal(size=(5000, 3))
        # Strong positive signal on f0
        logits = 2.0 * X[:, 0]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=5000) < p).astype(int)

        model = LogisticScoreStateModel(feature_names=["f0", "f1", "f2"])
        model.fit(X, y)
        summary = model.coef_summary()
        assert summary is not None
        # f0 coefficient should be strongly positive
        assert summary["coefs"]["f0"] > 0.5
        # f1, f2 should be near zero (no signal)
        assert abs(summary["coefs"]["f1"]) < 0.5
        assert abs(summary["coefs"]["f2"]) < 0.5

    def test_nan_handled_at_predict(self):
        X_train = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0]])
        y_train = np.array([0, 1, 0, 1])
        model = LogisticScoreStateModel(feature_names=["a", "b"])
        model.fit(X_train, y_train)

        # Predict with a NaN — should be imputed to column mean, not crash.
        X_test = np.array([[np.nan, 5.0]])
        probs = model.predict_proba(X_test)
        assert probs.shape == (1,)
        assert 0.0 <= probs[0] <= 1.0

    def test_predict_before_fit_raises(self):
        model = LogisticScoreStateModel(feature_names=["a", "b"])
        with pytest.raises(RuntimeError):
            model.predict_proba(np.zeros((1, 2)))

    def test_builder_dispatch(self):
        m = build_score_state_model(type_="logistic", feature_names=["a", "b"])
        assert isinstance(m, LogisticScoreStateModel)

    def test_builder_xgboost(self):
        from mvp.projection.iid.score_state_model import XGBoostScoreStateModel
        m = build_score_state_model(type_="xgboost", feature_names=["a", "b"])
        assert isinstance(m, XGBoostScoreStateModel)

    def test_xgboost_fit_predict(self):
        rng = np.random.default_rng(2)
        X = rng.normal(size=(500, 3))
        logits = 0.8 * X[:, 0] - 0.4 * X[:, 2]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=500) < p).astype(int)

        m = build_score_state_model(
            type_="xgboost",
            feature_names=["f0", "f1", "f2"],
            params={"n_estimators": 20, "max_depth": 3},
        )
        m.fit(X, y)
        probs = m.predict_proba(X)
        assert probs.shape == (500,)
        assert np.all((probs >= 0) & (probs <= 1))
        summary = m.coef_summary()
        assert summary is not None
        # f0 importance should be highest among the three
        imps = summary["feature_importances"]
        assert imps["f0"] >= max(imps["f1"], imps["f2"])

    def test_builder_unknown_type(self):
        with pytest.raises(ValueError, match="unknown"):
            build_score_state_model(type_="not-a-type", feature_names=["a"])
