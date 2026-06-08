"""Model wrappers for experiments."""


import os
from abc import ABC, abstractmethod
from typing import Any

import numpy as np

_n_jobs_override: int | None = None


def set_n_jobs_override(n: int | None) -> None:
    """Set a global n_jobs override for all models."""
    global _n_jobs_override
    _n_jobs_override = n


def get_n_jobs_override() -> int | None:
    """Return the global n_jobs override, or None if unset (CLI --n-jobs)."""
    return _n_jobs_override


def _default_n_jobs() -> int:
    """Return n_jobs: global override if set, else cpu_count - 2."""
    if _n_jobs_override is not None:
        return _n_jobs_override
    cpu_count = os.cpu_count() or 4
    return max(1, cpu_count - 2)


def _fit_median_imputer(X: np.ndarray) -> np.ndarray:
    """Per-column training median (NaN-safe); all-NaN columns fall back to 0.

    For wrappers around sklearn / PyTorch models that can't accept NaN input.
    XGBoost handles NaN natively and does not need this.
    """
    medians = np.nanmedian(X, axis=0)
    return np.where(np.isnan(medians), 0.0, medians)


def _apply_median_imputer(X: np.ndarray, medians: np.ndarray) -> np.ndarray:
    """Replace NaN with the corresponding column median. Returns a copy if
    any NaN were present, else returns X unchanged."""
    if not np.isnan(X).any():
        return X
    out = X.copy()
    inds = np.where(np.isnan(out))
    out[inds] = medians[inds[1]]
    return out


def _resolve_monotone_constraints(
    params: dict[str, Any],
    feature_names: list[str] | None,
) -> dict[str, Any]:
    """Resolve dict-form `monotone_constraints` to a positional tuple.

    XGBoost accepts `monotone_constraints` as a Mapping[str, int] only when
    training data carries column names. Our pipeline passes nameless numpy
    arrays (runner builds X via `to_numpy()`), so dict-by-name never binds.
    This converts {feat_name: ±1} into a tuple aligned with `feature_names`.

    Pass-through when monotone_constraints is absent, already positional
    (tuple/list/str), or feature_names is None and the value isn't a dict.
    """
    if "monotone_constraints" not in params:
        return params
    mc = params["monotone_constraints"]
    if not isinstance(mc, dict):
        return params
    if feature_names is None:
        raise ValueError(
            "monotone_constraints dict form requires feature_names; "
            "use a positional tuple/string if feature_names is unavailable"
        )
    unknown = [k for k in mc if k not in feature_names]
    if unknown:
        raise ValueError(
            f"monotone_constraints references unknown features: {unknown}. "
            f"Available: {feature_names}"
        )
    tuple_form = tuple(int(mc.get(name, 0)) for name in feature_names)
    out = dict(params)
    out["monotone_constraints"] = tuple_form
    return out


class BaseModel(ABC):
    """Base class for model wrappers."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        """Fit the model."""
        pass

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict probabilities for positive class."""
        pass


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


def _asymmetric_logloss(
    y_true: np.ndarray, y_pred: np.ndarray, lambda_over: float
) -> tuple[np.ndarray, np.ndarray]:
    """XGB custom objective: log-loss with the overconfident side weighted
    by `lambda_over` (>=1). y_pred arrives as raw margin (logit) under
    XGB's custom-objective contract; we apply sigmoid here.

    Overconfident = sigmoid(pred) > y (predicting high when actual is low).

    Module-level (not a closure) so functools.partial wrapping is picklable —
    XGBClassifier stores the objective on the booster and joblib must
    serialize it when we save the trained model artifact.
    """
    p = _sigmoid(y_pred)
    weight = np.where(p > y_true, lambda_over, 1.0)
    grad = (p - y_true) * weight
    hess = p * (1.0 - p) * weight
    return grad, hess


def _asymmetric_logloss_factory(lambda_over: float):
    """Return a picklable callable bound to lambda_over for XGB's `objective=`."""
    import functools
    return functools.partial(_asymmetric_logloss, lambda_over=lambda_over)


def _mtl_heterogeneous_objective(
    predt: np.ndarray,
    dtrain: Any,  # xgb.DMatrix
    weights: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """XGB custom objective: heterogeneous multi-task loss.

    Column 0 of `predt` is the primary binary-classification head: standard
    logistic gradient/Hessian on raw margin.
    Columns 1+ are regression heads on STANDARDIZED auxiliary targets
    (standardization is applied by the model wrapper before DMatrix
    construction): standard squared-error gradient/Hessian.

    Per-target gradient and Hessian are scaled by `weights[i]`. The per-row
    Hessian is diagonal across targets (each (row, target) entry depends only
    on that target's prediction) — required by XGBoost's multi-output
    custom-objective contract.

    `dtrain.get_label()` returns the label buffer as 1D; reshape to predt's
    shape `[n_rows, num_target]` (row-major matches XGBoost's internal layout).

    Module-level so functools.partial wraps it picklably for `xgb.train(obj=)`.
    """
    y = dtrain.get_label().reshape(predt.shape)

    grad = np.empty_like(predt)
    hess = np.empty_like(predt)

    # Primary (col 0): logistic on raw margin.
    p = _sigmoid(predt[:, 0])
    grad[:, 0] = (p - y[:, 0]) * weights[0]
    hess[:, 0] = p * (1.0 - p) * weights[0]

    # Aux (cols 1+): squared error on standardized targets. Constant Hessian.
    for i in range(1, predt.shape[1]):
        grad[:, i] = (predt[:, i] - y[:, i]) * weights[i]
        hess[:, i] = weights[i]

    return grad, hess


def _mtl_heterogeneous_objective_factory(weights: np.ndarray):
    """Return a picklable callable bound to per-target loss weights."""
    import functools
    return functools.partial(_mtl_heterogeneous_objective, weights=weights)


def _mtl_primary_logloss_eval(
    predt: np.ndarray,
    dtrain: Any,  # xgb.DMatrix
) -> tuple[str, float]:
    """XGB custom eval metric: log-loss on the primary head only.

    `predt` shape is `[n_rows, num_target]`; extract column 0 (primary raw
    margin), sigmoid-transform, compute binary log-loss against the primary
    label column. This is the multi-output analog of XGBoost's built-in
    `binary:logistic` eval metric — same numerical signal, just operating on
    a 2D prediction matrix.

    Used by the MTL training path so early stopping is symmetric with the
    single-task baseline's `binary:logistic` eval metric.

    Module-level for picklability (factories not required since this takes no
    bound parameters).
    """
    y = dtrain.get_label().reshape(predt.shape)
    p = _sigmoid(predt[:, 0])
    eps = 1e-15
    p_clip = np.clip(p, eps, 1.0 - eps)
    ll = -float(np.mean(
        y[:, 0] * np.log(p_clip) + (1.0 - y[:, 0]) * np.log(1.0 - p_clip)
    ))
    return ("primary_logloss", ll)


class XGBoostModel(BaseModel):
    """XGBoost classifier wrapper."""

    def __init__(
        self,
        params: dict[str, Any],
        feature_names: list[str] | None = None,
    ) -> None:
        resolved = _resolve_monotone_constraints(params, feature_names)
        # Custom objective: keep "objective": "asymmetric_logloss" as a string
        # in self.params so the config snapshot can yaml-dump it. Stash
        # lambda_over separately; we materialize the callable only at fit
        # time when building XGBClassifier kwargs.
        self._lambda_over: float | None = None
        if resolved.get("objective") == "asymmetric_logloss":
            # dict(resolved) so we don't mutate the caller's params
            resolved = dict(resolved)
            self._lambda_over = float(resolved.pop("lambda_over", 2.0))
        # DART-only params have no effect under gbtree and XGBoost warns about
        # them every fit. The tuner samples them unconditionally per trial, so
        # drop them here when the chosen booster isn't dart.
        if resolved.get("booster", "gbtree") != "dart":
            resolved = {k: v for k, v in resolved.items() if k not in ("rate_drop", "skip_drop")}
        # tree_method="exact" doesn't support: colsample_bynode (hard error),
        # grow_policy=lossguide (lossguide requires histogram-based splits),
        # or max_bin (histogram-only). Force compatible values rather than
        # failing the trial.
        if resolved.get("tree_method") == "exact":
            resolved = {k: v for k, v in resolved.items() if k not in ("colsample_bynode", "max_bin")}
            if resolved.get("grow_policy") == "lossguide":
                resolved["grow_policy"] = "depthwise"
        self.params = {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "n_jobs": _default_n_jobs(),
            "random_state": 42,
            **resolved,
        }
        self._model = None

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None = None,
        early_stopping_rounds: int | None = 10,
    ) -> None:
        import xgboost as xgb

        # Build XGB kwargs separately so self.params stays serializable —
        # the callable lives only on the XGBClassifier (and gets pickled
        # with it via functools.partial, which IS picklable since it wraps
        # a module-level function).
        xgb_params = dict(self.params)
        if self._lambda_over is not None:
            xgb_params["objective"] = _asymmetric_logloss_factory(self._lambda_over)
        self._model = xgb.XGBClassifier(**xgb_params)
        fit_kwargs: dict[str, Any] = {"sample_weight": sample_weight}
        if eval_set is not None:
            fit_kwargs["eval_set"] = eval_set
            fit_kwargs["verbose"] = False
            if early_stopping_rounds is not None:
                self._model.set_params(early_stopping_rounds=early_stopping_rounds)
        self._model.fit(X, y, **fit_kwargs)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        # With a custom objective, XGBClassifier's predict_proba returns raw
        # margin (logits) rather than probabilities, because XGB no longer
        # knows the output space. Apply sigmoid ourselves in that case.
        # getattr fallback handles artifacts pickled before commit 77a69b3
        # added _lambda_over to __init__ — old joblibs deserialize without
        # the attribute and must default to the standard-logistic path.
        lambda_over = getattr(self, "_lambda_over", None)
        if lambda_over is not None:
            raw = self._model.predict(X, output_margin=True)
            return _sigmoid(raw)
        return self._model.predict_proba(X)[:, 1]


class XGBoostMTLModel(BaseModel):
    """XGBoost multi-task model: vector-leaf trees + custom heterogeneous objective.

    Trains a single booster that produces predictions for all targets jointly.
    Column 0 of the output is the primary binary classification head (raw
    margin / logit, sigmoid-transformed in `predict_proba`). Columns 1+ are
    regression heads for auxiliary targets — internally trained against
    STANDARDIZED labels so the per-target loss weights express importance
    rather than scale.

    `predict_proba(X)` returns the primary head as a 1D probability array,
    matching `BaseModel`'s contract — downstream code that called single-task
    `predict_proba` works without change. `predict_aux(X)` returns aux head
    predictions on the ORIGINAL (un-standardized) scale, for R² reporting and
    aux-head sanity checks.

    Per-target loss weights are read from `params` under keys
    `weight_{target_name}` (e.g. `weight_won`, `weight_game_margin`). When a
    weight key is absent, defaults are 1.0 for the primary (target_names[0])
    and 0.1 for aux targets. The Optuna HP sweep populates these keys when
    tuning loss weights as additional HP dimensions.

    Standardization parameters (per-aux mean and std) are computed on the
    training fold only and cached on the model; they're serialized with the
    pickled artifact so reload + inverse transform later use the original
    training-fold parameters rather than recomputing on new data.
    """

    def __init__(
        self,
        params: dict[str, Any],
        target_names: list[str],
        feature_names: list[str] | None = None,
    ) -> None:
        if not target_names:
            raise ValueError("target_names must include at least the primary target")
        self.target_names = list(target_names)
        self.num_target = len(target_names)

        resolved = _resolve_monotone_constraints(params, feature_names)
        resolved = dict(resolved)  # don't mutate caller

        # Extract per-target loss weights. Pop them out of `resolved` so they
        # don't leak through to XGBoost as unknown parameters.
        weights = []
        for i, name in enumerate(self.target_names):
            key = f"weight_{name}"
            default = 1.0 if i == 0 else 0.1
            weights.append(float(resolved.pop(key, default)))
        self.loss_weights = np.asarray(weights, dtype=np.float64)

        # Defensive: drop any remaining weight_* keys (MTL-specific config for
        # targets not present in target_names — e.g., stale config from a
        # previous MTLConfig.auxiliary_targets value). XGBoost would otherwise
        # emit a "parameters not used" warning for each.
        for stale_key in [k for k in resolved if k.startswith("weight_")]:
            resolved.pop(stale_key)

        # DART-only params have no effect under gbtree and XGBoost warns about
        # them every fit. The tuner samples them unconditionally per trial.
        if resolved.get("booster", "gbtree") != "dart":
            resolved = {k: v for k, v in resolved.items() if k not in ("rate_drop", "skip_drop")}
        # multi_strategy=multi_output_tree (vector leaf) is only supported under
        # tree_method=hist (gbtree.cc:205); exact/approx hard-error. The tuner
        # samples tree_method unconditionally, so drop it here and let the hist
        # default below stand rather than failing the trial. Under hist the
        # exact-only incompatibilities (colsample_bynode, max_bin, lossguide)
        # don't apply, so nothing else needs coercing.
        resolved.pop("tree_method", None)

        # n_estimators is consumed by xgb.train as num_boost_round, not a
        # `params` entry. Pop it out the same way XGBoostModel does.
        self._n_estimators = int(resolved.pop("n_estimators", 100))

        self.params = {
            "tree_method": "hist",
            "multi_strategy": "multi_output_tree",
            "num_target": self.num_target,
            "n_jobs": _default_n_jobs(),
            "random_state": 42,
            "disable_default_eval_metric": 1,
            **resolved,
        }

        # Standardization parameters (filled at fit time).
        self._aux_mean: np.ndarray | None = None  # shape [num_aux]
        self._aux_std: np.ndarray | None = None
        self._booster = None

    def _standardize_aux(self, y: np.ndarray) -> np.ndarray:
        """Standardize the aux columns of y; primary column kept as-is.

        Requires `self._aux_mean` and `self._aux_std` to be set (i.e., fit()
        has been called or this is being applied to eval data after fit).
        Returns a copy; does not mutate the input.
        """
        out = y.astype(np.float64).copy()
        if self.num_target > 1:
            assert self._aux_mean is not None and self._aux_std is not None
            out[:, 1:] = (out[:, 1:] - self._aux_mean) / self._aux_std
        return out

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None = None,
        early_stopping_rounds: int | None = 10,
    ) -> None:
        import xgboost as xgb

        if y.ndim != 2:
            raise ValueError(
                f"XGBoostMTLModel.fit expects 2D y of shape [n_rows, num_target]; "
                f"got shape {y.shape}"
            )
        if y.shape[1] != self.num_target:
            raise ValueError(
                f"y has {y.shape[1]} target columns; "
                f"target_names has {self.num_target}"
            )

        # Fit standardization on the training fold's aux columns. Primary
        # stays as 0/1; only aux gets standardized.
        if self.num_target > 1:
            aux = y[:, 1:].astype(np.float64)
            self._aux_mean = aux.mean(axis=0)
            std = aux.std(axis=0)
            # Guard against zero std (degenerate aux column — e.g., all same
            # value). Substitute 1.0 so the standardization is a no-op for that
            # column, which is the right behavior: nothing to learn from a
            # constant.
            self._aux_std = np.where(std == 0.0, 1.0, std)
        else:
            self._aux_mean = np.zeros(0, dtype=np.float64)
            self._aux_std = np.ones(0, dtype=np.float64)

        y_train = self._standardize_aux(y)
        dtrain = xgb.DMatrix(X, label=y_train)
        # Explicit set_weight: silent failure mode if omitted under multi-output.
        if sample_weight is not None:
            dtrain.set_weight(np.asarray(sample_weight, dtype=np.float64))

        evals: list[tuple[Any, str]] = []
        if eval_set is not None:
            for X_eval, y_eval in eval_set:
                if y_eval.ndim != 2 or y_eval.shape[1] != self.num_target:
                    raise ValueError(
                        "eval_set y must be 2D with num_target columns"
                    )
                d_eval = xgb.DMatrix(X_eval, label=self._standardize_aux(y_eval))
                evals.append((d_eval, "validation"))

        train_kwargs: dict[str, Any] = {
            "params": self.params,
            "dtrain": dtrain,
            "num_boost_round": self._n_estimators,
            "obj": _mtl_heterogeneous_objective_factory(self.loss_weights),
            "custom_metric": _mtl_primary_logloss_eval,
        }
        if evals:
            train_kwargs["evals"] = evals
            train_kwargs["verbose_eval"] = False
            if early_stopping_rounds is not None:
                train_kwargs["early_stopping_rounds"] = early_stopping_rounds

        self._booster = xgb.train(**train_kwargs)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return primary head P(win) as a 1D array (BaseModel contract)."""
        if self._booster is None:
            raise RuntimeError("Model not fitted")
        import xgboost as xgb
        raw = self._booster.predict(xgb.DMatrix(X))
        # Vector-leaf output is [n_rows, num_target]; degenerate to 1D if
        # num_target == 1 (treated defensively; the MTL path shouldn't be
        # constructed with num_target == 1).
        if raw.ndim == 1:
            return _sigmoid(raw)
        return _sigmoid(raw[:, 0])

    def predict_aux(self, X: np.ndarray) -> np.ndarray:
        """Return aux head predictions on the ORIGINAL (un-standardized) scale.

        Shape: `[n_rows, num_aux]` where `num_aux = num_target - 1`. Empty
        array when there are no aux targets.

        For R² reporting on auxiliary heads (sanity-check that aux heads
        actually learned something) and any diagnostic use of aux predictions.
        Not used at deployment.
        """
        if self._booster is None:
            raise RuntimeError("Model not fitted")
        if self.num_target == 1:
            return np.empty((X.shape[0], 0))
        import xgboost as xgb
        raw = self._booster.predict(xgb.DMatrix(X))
        assert self._aux_mean is not None and self._aux_std is not None
        return raw[:, 1:] * self._aux_std + self._aux_mean


class LogisticModel(BaseModel):
    """Logistic regression classifier wrapper.

    sklearn's LogisticRegression does not accept NaN inputs. When features
    declared ``impute=None`` reach this wrapper, they arrive as NaN; the
    wrapper computes per-column training medians and fills NaN internally
    at both fit and predict time.
    """

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "max_iter": 1000, **params}
        # sklearn 1.8+ deprecates the `penalty` keyword and infers penalty
        # type from l1_ratio (0=pure L2, 1=pure L1, intermediate=elasticnet).
        # Strip `penalty` if present (legacy configs / stale tune DBs) so we
        # don't trigger the FutureWarning and the inconsistency UserWarning
        # when penalty disagrees with l1_ratio.
        self.params.pop("penalty", None)
        # Derive solver from l1_ratio when not explicitly set: lbfgs is fast
        # but only handles pure L2 (l1_ratio == 0); saga handles the full
        # L1 / L2 / elasticnet spectrum.
        if "solver" not in self.params:
            l1_ratio = self.params.get("l1_ratio") or 0.0
            self.params["solver"] = "lbfgs" if l1_ratio == 0.0 else "saga"
        self._model = None
        self._impute_medians: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.linear_model import LogisticRegression

        self._impute_medians = _fit_median_imputer(X)
        X = _apply_median_imputer(X, self._impute_medians)
        self._model = LogisticRegression(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        # getattr fallback handles artifacts pickled before commit c405766
        # added _impute_medians to __init__ — old joblibs deserialize without
        # the attribute and must skip the imputer step (matches their original
        # behavior before the wrapper-level imputation was added).
        medians = getattr(self, "_impute_medians", None)
        if medians is not None:
            X = _apply_median_imputer(X, medians)
        return self._model.predict_proba(X)[:, 1]


class RandomForestModel(BaseModel):
    """Random forest classifier wrapper.

    sklearn's RandomForestClassifier does not accept NaN inputs. Same
    internal median-imputation as LogisticModel — see that docstring.
    """

    def __init__(self, params: dict[str, Any]) -> None:
        merged = {"random_state": 42, "n_jobs": _default_n_jobs(), **params}
        # sklearn rejects max_samples when bootstrap=False. The tuner samples
        # both dims unconditionally per trial, so strip max_samples when
        # bootstrap is off rather than failing the trial.
        if not merged.get("bootstrap", True) and "max_samples" in merged:
            del merged["max_samples"]
        self.params = merged
        self._model = None
        self._impute_medians: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.ensemble import RandomForestClassifier

        self._impute_medians = _fit_median_imputer(X)
        X = _apply_median_imputer(X, self._impute_medians)
        self._model = RandomForestClassifier(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        # See LogisticModel.predict_proba for the c405766 backward-compat note.
        medians = getattr(self, "_impute_medians", None)
        if medians is not None:
            X = _apply_median_imputer(X, medians)
        return self._model.predict_proba(X)[:, 1]


class _SklearnWrapper:
    """Sklearn-compatible wrapper for permutation importance."""

    _estimator_type = "classifier"

    def __init__(self, ensemble: "EnsembleModel") -> None:
        self._ensemble = ensemble
        self.classes_ = np.array([0, 1])

    def fit(self, X: np.ndarray, y: np.ndarray) -> "_SklearnWrapper":
        self._ensemble.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        probs = self._ensemble.predict_proba(X)
        return (probs >= 0.5).astype(int)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        probs = self._ensemble.predict_proba(X)
        return np.column_stack([1 - probs, probs])

    def __sklearn_tags__(self):
        from sklearn.base import BaseEstimator
        tags = BaseEstimator.__sklearn_tags__(self)
        tags.estimator_type = "classifier"
        return tags


class EnsembleModel(BaseModel):
    """Ensemble model that combines multiple base models."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params
        self._sub_models: list[BaseModel] = []
        self._feature_indices: list[list[int]] = []
        self._weights: np.ndarray = np.array([])
        strategy = params.get("strategy", "average")
        self._strategy = strategy
        self._fitted = False
        self._meta_model = None
        self._meta_model_params: dict[str, Any] = params.get("meta_model_params", {})
        self._meta_feature_names: list[str] = []
        self._meta_feature_indices: list[int] = []
        self._meta_scaler: tuple[np.ndarray, np.ndarray] | None = None
        self._sub_calibrators: list = []
        self._sub_cal_configs: list = []

    def configure(self, base_model_specs: list[dict[str, Any]]) -> None:
        """Set up sub-models from resolved specs.

        Each spec: {type, params, feature_indices: list[int], calibration?}
        """
        self._sub_models = []
        self._feature_indices = []
        self._sub_calibrators = []
        self._sub_cal_configs = []
        weights = []
        for spec in base_model_specs:
            sub = get_model(spec["type"], spec.get("params") or {})
            self._sub_models.append(sub)
            self._feature_indices.append(spec["feature_indices"])
            self._sub_cal_configs.append(spec.get("calibration"))
            self._sub_calibrators.append(None)
            weights.append(spec.get("weight", 1.0))
        w = np.array(weights, dtype=np.float64)
        self._weights = w / w.sum()

    def set_sub_calibrator(self, idx: int, calibrator) -> None:
        """Attach a fitted DEPLOYED calibrator to sub-model ``idx``.

        Called by the runner after fitting each sub's nested-CV calibrators.
        Only the deployed calibrator (fit on all OOF) is stored for inference;
        the nested fold-i-out calibrators are used inside
        ``fit_calibrator_with_nested_cv`` for honest diagnostics and discarded.

        ``None`` means raw passthrough for that sub.
        """
        self._sub_calibrators[idx] = calibrator

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
        per_model_data: (
            list[tuple[np.ndarray, np.ndarray, np.ndarray | None] | None] | None
        ) = None,
    ) -> None:
        if not self._sub_models:
            raise RuntimeError("EnsembleModel not configured. Call configure() first.")
        for i, (sub, indices) in enumerate(zip(self._sub_models, self._feature_indices)):
            if per_model_data and per_model_data[i] is not None:
                entry = per_model_data[i]
                X_sub, y_sub = entry[0], entry[1]
                w_sub = entry[2] if len(entry) > 2 else None
                sub.fit(X_sub[:, indices], y_sub, sample_weight=w_sub)
            else:
                sub.fit(X[:, indices], y, sample_weight=sample_weight)
        self._fitted = True

    def set_meta_feature_indices(self, indices: list[int]) -> None:
        """Store indices into X for meta-feature columns."""
        self._meta_feature_indices = indices

    def set_meta_feature_names(self, names: list[str]) -> None:
        """Store base model names for coefficient reporting."""
        self._meta_feature_names = names

    def fit_meta(self, X_meta: np.ndarray, y_meta: np.ndarray) -> None:
        """Fit stacking meta-model on out-of-fold base predictions."""
        if X_meta.shape[0] < 2:
            raise ValueError("Need at least 2 OOF samples to fit meta-model")
        from sklearn.linear_model import LogisticRegression

        self._meta_model = LogisticRegression(
            max_iter=1000, random_state=42, **self._meta_model_params
        )
        self._meta_model.fit(X_meta, y_meta)

    def get_meta_coefficients(self) -> tuple[float, dict[str, float]]:
        """Return meta-model intercept and per-base-model coefficients."""
        if self._meta_model is None:
            raise RuntimeError("Meta-model not fitted")
        intercept = float(self._meta_model.intercept_[0])
        coefs = {}
        for name, coef in zip(self._meta_feature_names, self._meta_model.coef_[0]):
            coefs[name] = float(coef)
        return intercept, coefs

    def predict_proba(self, X: np.ndarray, df=None) -> np.ndarray:
        """Ensemble prediction.

        ``df`` is forwarded to ``_predict_all`` and is required when any
        attached sub calibrator is segmented (segmented cal needs the
        polars df to derive segment labels). Non-segmented sub cals
        ignore ``df``.
        """
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        preds = self._predict_all(X, df=df)
        if self._strategy == "stacking":
            if self._meta_model is None:
                raise RuntimeError("Meta-model not fitted. Call fit_meta() first.")
            parts = list(preds)
            if self._meta_feature_indices:
                meta_feat = X[:, self._meta_feature_indices]
                if self._meta_scaler is not None:
                    mean, std = self._meta_scaler
                    meta_feat = (meta_feat - mean) / std
                parts.append(meta_feat)
            return self._meta_model.predict_proba(np.column_stack(parts))[:, 1]
        if self._strategy == "weighted_average":
            return np.average(preds, axis=0, weights=self._weights)
        return np.mean(preds, axis=0)

    def predict_proba_per_model(self, X: np.ndarray, df=None) -> list[np.ndarray]:
        """Return individual predictions from each sub-model.

        See ``predict_proba`` for ``df`` semantics.
        """
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        return self._predict_all(X, df=df)

    def _predict_all(self, X: np.ndarray, df=None) -> list[np.ndarray]:
        from mvp.model.calibration import (
            SegmentedIsotonicCalibrator,
            SegmentedPlattCalibrator,
        )

        outputs = []
        for i, (sub, idx) in enumerate(zip(self._sub_models, self._feature_indices)):
            raw = sub.predict_proba(X[:, idx])
            cal = self._sub_calibrators[i]
            if cal is None:
                outputs.append(raw)
                continue
            if isinstance(cal, (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator)):
                if df is None:
                    raise ValueError(
                        f"Sub-model {i} has a segmented calibrator "
                        f"({type(cal).__name__}) attached but predict_proba "
                        "was called without df. Pass df=<polars df> to "
                        "predict_proba / predict_proba_per_model."
                    )
                outputs.append(cal.transform(raw, df))
            else:
                outputs.append(cal.transform(raw))
        return outputs

    def __setstate__(self, state):
        """Backward compat for joblib artifacts pickled before per-sub cal.

        Old artifacts lack ``_sub_calibrators`` and ``_sub_cal_configs``.
        Without this patch, ``_predict_all`` would AttributeError when
        iterating ``self._sub_calibrators``. Initialize both to None-filled
        lists matching the existing sub-model count so ``_predict_all`` falls
        through to raw passthrough — i.e., bit-identical to pre-PR behavior.
        """
        self.__dict__.update(state)
        if not hasattr(self, "_sub_calibrators") or self._sub_calibrators is None:
            self._sub_calibrators = [None] * len(self._sub_models)
        if not hasattr(self, "_sub_cal_configs") or self._sub_cal_configs is None:
            self._sub_cal_configs = [None] * len(self._sub_models)

    @property
    def _model(self) -> _SklearnWrapper:
        """Sklearn-compatible wrapper for permutation importance."""
        return _SklearnWrapper(self)


def _make_activation(name: str):
    """Resolve activation name to a fresh torch.nn module instance."""
    import torch.nn as nn

    mapping = {
        "relu": nn.ReLU,
        "gelu": nn.GELU,
        "silu": nn.SiLU,
        "leaky_relu": nn.LeakyReLU,
    }
    if name not in mapping:
        raise ValueError(f"Unknown activation: {name}")
    return mapping[name]()


def _make_embedding_mlp():
    """Lazy factory to avoid top-level torch import."""
    import torch
    import torch.nn as nn

    class EmbeddingMLP(nn.Module):
        def __init__(
            self,
            n_features: int,
            n_players: int,
            embedding_dim: int,
            hidden_layers: list[int],
            dropout: float,
            batch_norm: bool,
            layer_norm: bool = False,
            dual_embedding: bool = False,
            activation: str = "relu",
        ):
            super().__init__()
            self.embedding = nn.Embedding(n_players + 1, embedding_dim, padding_idx=0)
            self.dual_embedding = dual_embedding
            n_emb = 2 * embedding_dim if dual_embedding else embedding_dim
            mlp_input_dim = n_features + n_emb
            layers: list[nn.Module] = []
            in_dim = mlp_input_dim
            for hidden_dim in hidden_layers:
                layers.append(nn.Linear(in_dim, hidden_dim))
                if layer_norm:
                    layers.append(nn.LayerNorm(hidden_dim))
                elif batch_norm:
                    layers.append(nn.BatchNorm1d(hidden_dim))
                layers.append(_make_activation(activation))
                if dropout > 0:
                    layers.append(nn.Dropout(dropout))
                in_dim = hidden_dim
            layers.append(nn.Linear(in_dim, 1))
            self.mlp = nn.Sequential(*layers)

        def forward(
            self,
            x: torch.Tensor,
            player_idx: torch.Tensor,
            opp_idx: torch.Tensor | None = None,
        ) -> torch.Tensor:
            player_emb = self.embedding(player_idx)
            parts = [x, player_emb]
            if opp_idx is not None:
                opp_emb = self.embedding(opp_idx)
                parts.append(opp_emb)
            combined = torch.cat(parts, dim=1)
            return self.mlp(combined)

    return EmbeddingMLP


class NeuralNetModel(BaseModel):
    """PyTorch MLP wrapper for tabular classification."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.hidden_layers: list[int] = params.get("hidden_layers", [64, 32])
        self.dropout: float = params.get("dropout", 0.3)
        self.learning_rate: float = params.get("learning_rate", 0.001)
        self.batch_size: int = params.get("batch_size", 512)
        self.epochs: int = params.get("epochs", 200)
        self.patience: int = params.get("patience", 15)
        self.batch_norm: bool = params.get("batch_norm", False)
        self.embedding_dim: int = params.get("embedding_dim", 0)
        self.embedding_col_idx: int | None = params.get("embedding_col_idx", None)
        self.opp_embedding_col_idx: int | None = params.get("opp_embedding_col_idx", None)
        self.n_players: int = params.get("n_players", 0)
        self.shuffle: bool = params.get("shuffle", True)
        self.finetune_frac: float = params.get("finetune_frac", 0.0)
        self.finetune_lr: float = params.get("finetune_lr", 0.0001)
        self.finetune_epochs: int = params.get("finetune_epochs", 100)
        self.finetune_patience: int = params.get("finetune_patience", 10)
        self.device: str | None = params.get("device", None)
        self.random_state: int | None = params.get("random_state", None)
        self.label_smoothing: float = params.get("label_smoothing", 0.0)
        self.weight_decay: float = params.get("weight_decay", 0.0)
        self.grad_clip_norm: float | None = params.get("grad_clip_norm", None)
        self.lr_scheduler: str | None = params.get("lr_scheduler", None)
        self.lr_scheduler_factor: float = params.get("lr_scheduler_factor", 0.5)
        self.lr_scheduler_patience: int = params.get("lr_scheduler_patience", 5)
        # "auto" = original Adam/AdamW heuristic from weight_decay; any other
        # value overrides explicitly (adam, adamw, sgd_momentum, radam, nadam).
        self.optimizer_type: str = params.get("optimizer", "auto")
        # Hidden-layer activation. "relu" preserves the original behavior;
        # gelu/silu/leaky_relu are alternatives the tuner can explore.
        self.activation: str = params.get("activation", "relu")
        self.layer_norm: bool = params.get("layer_norm", False)
        if self.batch_norm and self.layer_norm:
            raise ValueError(
                "batch_norm and layer_norm are mutually exclusive; enable only one"
            )
        if self.opp_embedding_col_idx is not None and not (
            self.embedding_dim > 0 and self.n_players > 0 and self.embedding_col_idx is not None
        ):
            raise ValueError(
                "opp_embedding_col_idx requires embedding_dim, embedding_col_idx, "
                "and n_players to also be set"
            )
        self._module = None
        self._device = None
        self._n_features = None
        self._impute_medians: np.ndarray | None = None

    @property
    def _has_embeddings(self) -> bool:
        return self.embedding_dim > 0 and self.n_players > 0 and self.embedding_col_idx is not None

    @property
    def _has_opp_embeddings(self) -> bool:
        return self._has_embeddings and self.opp_embedding_col_idx is not None

    def _build_module(self, n_features: int):
        import torch.nn as nn

        if self._has_embeddings:
            EmbeddingMLP = _make_embedding_mlp()
            return EmbeddingMLP(
                n_features=n_features,
                n_players=self.n_players,
                embedding_dim=self.embedding_dim,
                hidden_layers=self.hidden_layers,
                dropout=self.dropout,
                batch_norm=self.batch_norm,
                layer_norm=self.layer_norm,
                dual_embedding=self._has_opp_embeddings,
                activation=self.activation,
            )

        layers: list[nn.Module] = []
        in_dim = n_features
        for hidden_dim in self.hidden_layers:
            layers.append(nn.Linear(in_dim, hidden_dim))
            if self.layer_norm:
                layers.append(nn.LayerNorm(hidden_dim))
            elif self.batch_norm:
                layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(_make_activation(self.activation))
            if self.dropout > 0:
                layers.append(nn.Dropout(self.dropout))
            in_dim = hidden_dim
        layers.append(nn.Linear(in_dim, 1))
        return nn.Sequential(*layers)

    def _get_device(self):
        import torch

        if self.device is not None:
            if self.device == "cuda" and not torch.cuda.is_available():
                return torch.device("cpu")
            return torch.device(self.device)
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")

    def _split_embedding_col(
        self, X: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
        """Separate embedding ID column(s) from feature columns."""
        if not self._has_embeddings:
            return X, None, None
        cols_to_remove = [self.embedding_col_idx]
        opp_ids = None
        if self._has_opp_embeddings:
            cols_to_remove.append(self.opp_embedding_col_idx)
            opp_ids = X[:, self.opp_embedding_col_idx].astype(int)
        emb_ids = X[:, self.embedding_col_idx].astype(int)
        X_features = np.delete(X, cols_to_remove, axis=1)
        return X_features, emb_ids, opp_ids

    def _make_optimizer(self, params, lr: float):
        """Create optimizer based on self.optimizer_type.

        "auto" preserves the original behavior: AdamW when weight_decay > 0,
        else Adam. Other choices override that selection. SGD-momentum uses
        a fixed momentum=0.9 (the textbook value); the tuner explores SGD
        vs adaptive optimizers, not the SGD momentum coefficient itself.
        """
        import torch

        wd = self.weight_decay
        opt = self.optimizer_type
        if opt == "auto":
            if wd > 0:
                return torch.optim.AdamW(params, lr=lr, weight_decay=wd)
            return torch.optim.Adam(params, lr=lr)
        if opt == "adam":
            return torch.optim.Adam(params, lr=lr, weight_decay=wd)
        if opt == "adamw":
            return torch.optim.AdamW(params, lr=lr, weight_decay=wd)
        if opt == "sgd_momentum":
            return torch.optim.SGD(params, lr=lr, momentum=0.9, weight_decay=wd)
        if opt == "radam":
            return torch.optim.RAdam(params, lr=lr, weight_decay=wd)
        if opt == "nadam":
            return torch.optim.NAdam(params, lr=lr, weight_decay=wd)
        raise ValueError(f"Unknown optimizer: {opt}")

    def _forward(self, X_t, emb_t=None, opp_emb_t=None):
        """Forward pass handling plain MLP, single embedding, and dual embedding."""
        if emb_t is not None:
            return self._module(X_t, emb_t, opp_emb_t)
        return self._module(X_t)

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        import torch
        from torch.utils.data import DataLoader, TensorDataset

        if self.random_state is not None:
            torch.manual_seed(self.random_state)
        self._device = self._get_device()

        # Separate embedding column before counting features
        X_features, emb_ids, opp_emb_ids = self._split_embedding_col(X)
        # Median-impute NaN on the feature matrix only (embedding IDs are
        # integer keys and shouldn't be median-imputed). Medians fit once
        # on training, reused at predict time.
        self._impute_medians = _fit_median_imputer(X_features)
        X_features = _apply_median_imputer(X_features, self._impute_medians)
        self._n_features = X_features.shape[1]
        self._module = self._build_module(self._n_features).to(self._device)

        # Temporal train/val split (last 15%)
        val_size = max(1, int(len(X_features) * 0.15))
        X_train, X_val = X_features[:-val_size], X_features[-val_size:]
        y_train, y_val = y[:-val_size], y[-val_size:]
        w_train = sample_weight[:-val_size] if sample_weight is not None else None

        X_t = torch.tensor(X_train, dtype=torch.float32, device=self._device)
        y_t = torch.tensor(y_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        X_v = torch.tensor(X_val, dtype=torch.float32, device=self._device)
        y_v = torch.tensor(y_val, dtype=torch.float32, device=self._device).unsqueeze(1)

        # Embedding index tensors
        if emb_ids is not None:
            emb_t = torch.tensor(emb_ids[:-val_size], dtype=torch.long, device=self._device)
            emb_v = torch.tensor(emb_ids[-val_size:], dtype=torch.long, device=self._device)
        else:
            emb_t = None
            emb_v = None
        if opp_emb_ids is not None:
            opp_emb_t = torch.tensor(opp_emb_ids[:-val_size], dtype=torch.long, device=self._device)
            opp_emb_v = torch.tensor(opp_emb_ids[-val_size:], dtype=torch.long, device=self._device)
        else:
            opp_emb_t = None
            opp_emb_v = None

        if w_train is not None:
            w_t = torch.tensor(w_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        else:
            w_t = None

        optimizer = self._make_optimizer(self._module.parameters(), self.learning_rate)
        scheduler = None
        if self.lr_scheduler == "plateau":
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer,
                mode="min",
                factor=self.lr_scheduler_factor,
                patience=self.lr_scheduler_patience,
            )
        loss_fn = torch.nn.BCEWithLogitsLoss(reduction="none")

        # Build dataset with optional embedding and weight tensors
        tensors = [X_t, y_t]
        if emb_t is not None:
            tensors.append(emb_t)
        if opp_emb_t is not None:
            tensors.append(opp_emb_t)
        if w_t is not None:
            tensors.append(w_t)
        dataset = TensorDataset(*tensors)
        # drop_last when batch_norm is on: BatchNorm needs batch size > 1 to
        # compute variance, and a final batch of size 1 (when n_train % bs == 1)
        # crashes. LayerNorm and no-norm handle batch size 1 fine, so they
        # keep all data.
        loader = DataLoader(
            dataset, batch_size=self.batch_size, shuffle=self.shuffle,
            drop_last=self.batch_norm,
        )

        best_val_loss = float("inf")
        best_state = None
        wait = 0
        has_emb = emb_t is not None
        has_w = w_t is not None
        has_opp_emb = opp_emb_t is not None

        for _epoch in range(self.epochs):
            self._module.train()
            for batch in loader:
                idx = 0
                xb = batch[idx]
                idx += 1
                yb = batch[idx]
                idx += 1
                eb = batch[idx] if has_emb else None
                if has_emb:
                    idx += 1
                oeb = batch[idx] if has_opp_emb else None
                if has_opp_emb:
                    idx += 1
                wb = batch[idx] if has_w else None

                if self.label_smoothing > 0:
                    yb = yb * (1 - self.label_smoothing) + 0.5 * self.label_smoothing
                optimizer.zero_grad()
                pred = self._forward(xb, eb, oeb)
                loss = loss_fn(pred, yb)
                if wb is not None:
                    loss = (loss * wb).mean()
                else:
                    loss = loss.mean()
                loss.backward()
                if self.grad_clip_norm is not None:
                    torch.nn.utils.clip_grad_norm_(
                        self._module.parameters(), max_norm=self.grad_clip_norm
                    )
                optimizer.step()

            # Validation
            self._module.eval()
            with torch.no_grad():
                val_pred = self._forward(X_v, emb_v, opp_emb_v)
                val_loss = loss_fn(val_pred, y_v).mean().item()

            if scheduler is not None:
                scheduler.step(val_loss)
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {
                    k: v.cpu().clone() for k, v in self._module.state_dict().items()
                }
                wait = 0
            else:
                wait += 1
                if wait >= self.patience:
                    break

        if best_state is not None:
            self._module.load_state_dict(best_state)

        # Phase 2: fine-tune on recent data only
        if self.finetune_frac > 0:
            n_total = len(X_train)
            n_recent = max(1, int(n_total * self.finetune_frac))
            ft_X = X_t[-n_recent:]
            ft_y = y_t[-n_recent:]
            ft_emb = emb_t[-n_recent:] if emb_t is not None else None
            ft_opp_emb = opp_emb_t[-n_recent:] if opp_emb_t is not None else None
            ft_w = w_t[-n_recent:] if w_t is not None else None

            ft_tensors = [ft_X, ft_y]
            if ft_emb is not None:
                ft_tensors.append(ft_emb)
            if ft_opp_emb is not None:
                ft_tensors.append(ft_opp_emb)
            if ft_w is not None:
                ft_tensors.append(ft_w)
            ft_dataset = TensorDataset(*ft_tensors)
            ft_loader = DataLoader(
                ft_dataset, batch_size=self.batch_size, shuffle=True,
                drop_last=self.batch_norm,
            )

            ft_optimizer = self._make_optimizer(self._module.parameters(), self.finetune_lr)
            ft_scheduler = None
            if self.lr_scheduler == "plateau":
                ft_scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                    ft_optimizer,
                    mode="min",
                    factor=self.lr_scheduler_factor,
                    patience=self.lr_scheduler_patience,
                )
            best_val_loss = float("inf")
            best_state = None
            wait = 0

            for _epoch in range(self.finetune_epochs):
                self._module.train()
                for batch in ft_loader:
                    idx = 0
                    xb = batch[idx]
                    idx += 1
                    yb = batch[idx]
                    idx += 1
                    eb = batch[idx] if has_emb else None
                    if has_emb:
                        idx += 1
                    oeb = batch[idx] if has_opp_emb else None
                    if has_opp_emb:
                        idx += 1
                    wb = batch[idx] if has_w else None

                    if self.label_smoothing > 0:
                        yb = yb * (1 - self.label_smoothing) + 0.5 * self.label_smoothing
                    ft_optimizer.zero_grad()
                    pred = self._forward(xb, eb, oeb)
                    loss = loss_fn(pred, yb)
                    if wb is not None:
                        loss = (loss * wb).mean()
                    else:
                        loss = loss.mean()
                    loss.backward()
                    if self.grad_clip_norm is not None:
                        torch.nn.utils.clip_grad_norm_(
                            self._module.parameters(), max_norm=self.grad_clip_norm
                        )
                    ft_optimizer.step()

                self._module.eval()
                with torch.no_grad():
                    val_pred = self._forward(X_v, emb_v, opp_emb_v)
                    val_loss = loss_fn(val_pred, y_v).mean().item()

                if ft_scheduler is not None:
                    ft_scheduler.step(val_loss)
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    best_state = {
                        k: v.cpu().clone() for k, v in self._module.state_dict().items()
                    }
                    wait = 0
                else:
                    wait += 1
                    if wait >= self.finetune_patience:
                        break

            if best_state is not None:
                self._module.load_state_dict(best_state)

        self._module.eval()

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        import torch

        if self._module is None:
            raise RuntimeError("Model not fitted")
        self._module.eval()
        with torch.no_grad():
            X_features, emb_ids, opp_emb_ids = self._split_embedding_col(X)
            # See LogisticModel.predict_proba for the c405766 backward-compat note.
            medians = getattr(self, "_impute_medians", None)
            if medians is not None:
                X_features = _apply_median_imputer(X_features, medians)
            X_t = torch.tensor(X_features, dtype=torch.float32, device=self._device)
            emb_t = None
            opp_emb_t = None
            if emb_ids is not None:
                emb_t = torch.tensor(emb_ids, dtype=torch.long, device=self._device)
            if opp_emb_ids is not None:
                opp_emb_t = torch.tensor(opp_emb_ids, dtype=torch.long, device=self._device)
            preds = torch.sigmoid(self._forward(X_t, emb_t, opp_emb_t)).squeeze(1).cpu().numpy()
        return preds

    def __getstate__(self):
        state = self.__dict__.copy()
        if self._module is not None:
            state["_module_state_dict"] = {
                k: v.cpu() for k, v in self._module.state_dict().items()
            }
        else:
            state["_module_state_dict"] = None
        state["_module"] = None
        state["_device"] = None
        return state

    def __setstate__(self, state):
        module_state = state.pop("_module_state_dict", None)
        self.__dict__.update(state)
        self._device = self._get_device()
        if module_state is not None and self._n_features is not None:
            self._module = self._build_module(self._n_features).to(self._device)
            self._module.load_state_dict(
                {k: v.to(self._device) for k, v in module_state.items()}
            )
            self._module.eval()


def get_model(
    model_type: str,
    params: dict[str, Any],
    feature_names: list[str] | None = None,
) -> BaseModel:
    """Factory function to get model wrapper.

    Args:
        model_type: Type of model ("xgboost", "logistic", "random_forest").
        params: Model parameters.
        feature_names: Feature names in training-matrix column order. Only
            consumed by XGBoost to resolve dict-form `monotone_constraints`.

    Returns:
        Model wrapper instance.

    Raises:
        ValueError: If model type is unknown.
    """
    if model_type == "xgboost":
        return XGBoostModel(params, feature_names=feature_names)
    elif model_type == "logistic":
        return LogisticModel(params)
    elif model_type == "random_forest":
        return RandomForestModel(params)
    elif model_type == "ensemble":
        return EnsembleModel(params)
    elif model_type == "neural_net":
        return NeuralNetModel(params)
    elif model_type == "sequence":
        from mvp.model.sequence_model import SequenceModel
        return SequenceModel(params)
    else:
        raise ValueError(f"Unknown model type: {model_type}")
