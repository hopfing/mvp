"""Model hyperparameter tuning via Optuna Bayesian optimization."""

import gc
import logging
import tempfile
import time
import warnings
from pathlib import Path
from typing import Any

import optuna
import yaml
from optuna.exceptions import ExperimentalWarning

from mvp.common.base_job import get_data_root
from mvp.model.metrics import MAXIMIZE_METRICS as _MODEL_MAXIMIZE_METRICS
from mvp.model.models import _default_n_jobs
from mvp.projection.iid.metric_registry import METRICS as _IID_METRICS

logger = logging.getLogger(__name__)

_PROJECTION_MODEL_TYPES = {"xgb_regressor", "linear", "ridge"}

# Maximize metrics: the classification set (single-sourced from metrics.py,
# includes the tail-sensitive ranking objectives weighted_concordance /
# partial_auc_tail), plus projection/IID extras — r_squared and the
# point-grain variants (re-emitted with a "point_" prefix by the score-state
# serve model) for any registry entry whose direction is "maximize".
_MAXIMIZE_METRICS = (
    _MODEL_MAXIMIZE_METRICS
    | {"r_squared"}
    | {
        f"point_{name}"
        for name, spec in _IID_METRICS.items()
        if spec.direction == "maximize"
    }
)

def _is_iid_config(raw: dict) -> bool:
    return isinstance(raw.get("serve_model"), dict)

DEFAULT_SEARCH_SPACES: dict[str, dict[str, dict[str, Any]]] = {
    "xgb_regressor": {
        "max_depth": {"type": "int", "low": 3, "high": 5},
        "learning_rate": {"type": "float", "low": 0.03, "high": 0.15, "log": True},
        "n_estimators": {"type": "int", "low": 100, "high": 500, "step": 50},
        "min_child_weight": {"type": "int", "low": 5, "high": 20},
        "subsample": {"type": "float", "low": 0.5, "high": 1.0},
        "colsample_bytree": {"type": "float", "low": 0.5, "high": 1.0},
        "colsample_bylevel": {"type": "float", "low": 0.5, "high": 1.0},
        "colsample_bynode": {"type": "float", "low": 0.5, "high": 1.0},
        "gamma": {"type": "float", "low": 0.0, "high": 5.0},
        "reg_alpha": {"type": "float", "low": 0.0, "high": 1.0},
        "reg_lambda": {"type": "float", "low": 0.5, "high": 10.0, "log": True},
        "max_delta_step": {"type": "int", "low": 0, "high": 5},
    },
    "xgboost": {
        "max_depth": {"type": "int", "low": 3, "high": 8},
        "learning_rate": {"type": "float", "low": 0.01, "high": 0.15, "log": True},
        "n_estimators": {"type": "int", "low": 100, "high": 1000, "step": 50},
        "min_child_weight": {"type": "int", "low": 1, "high": 20},
        "subsample": {"type": "float", "low": 0.5, "high": 1.0},
        "colsample_bytree": {"type": "float", "low": 0.5, "high": 1.0},
        "colsample_bylevel": {"type": "float", "low": 0.5, "high": 1.0},
        # tree_method: how splits are searched. hist (default) uses binned
        # histograms; approx uses quantile sketches. `exact` (evaluate every
        # split value) was dropped after a tuning-vs-holdout read across the
        # log_ds2412 studies: it was accuracy-neutral — its best trials tied
        # hist/approx within fold noise on both the tuning and held-out folds,
        # and it carried the smallest in/out gap (not an overfit risk) — while
        # being by far the slowest method. Placed before the params that
        # depend on it (colsample_bynode, grow_policy, max_bin) so the
        # conditional sampler sees its controller first.
        "tree_method": {"type": "categorical", "choices": ["hist", "approx"]},
        # colsample_bynode is unsupported under tree_method=exact, so only
        # sample it under hist/approx (the wrapper would otherwise strip it,
        # leaving an inert value in the winning config).
        "colsample_bynode": {
            "type": "float", "low": 0.5, "high": 1.0,
            "condition": {"param": "tree_method", "in": ["hist", "approx"]},
        },
        "gamma": {"type": "float", "low": 0.0, "high": 10.0},
        "reg_alpha": {"type": "float", "low": 0.0, "high": 1.0},
        "reg_lambda": {"type": "float", "low": 0.1, "high": 10.0, "log": True},
        "max_delta_step": {"type": "int", "low": 0, "high": 5},
        "scale_pos_weight": {"type": "float", "low": 0.9, "high": 1.1},
        # grow_policy: depthwise = balanced trees (max_depth is the binding
        # control); lossguide = split the leaf with highest loss reduction
        # next regardless of depth (max_leaves becomes binding). lossguide
        # is what LightGBM does by default.
        # lossguide requires a histogram-based tree_method; condition on
        # hist/approx so the tuner never samples the invalid exact+lossguide
        # pair (the wrapper would otherwise coerce lossguide to depthwise,
        # leaving a misleading value in the winning config).
        "grow_policy": {
            "type": "categorical", "choices": ["depthwise", "lossguide"],
            "condition": {"param": "tree_method", "in": ["hist", "approx"]},
        },
        # max_leaves: cap on total leaves per tree. 0=no limit (fine for
        # depthwise, where max_depth caps the tree shape). Constraining
        # makes lossguide grow narrower trees focused on high-loss regions.
        # Conditional: only the binding control under lossguide, so don't spend
        # a TPE dimension on it when grow_policy=depthwise.
        "max_leaves": {
            "type": "int", "low": 0, "high": 256, "step": 16,
            "condition": {"param": "grow_policy", "in": ["lossguide"]},
        },
        # max_bin: histogram bins for tree_method=hist (and approx). More
        # bins = finer split candidates but slower and more memory.
        # Conditional: tree_method=exact doesn't bin, so max_bin is inert there.
        "max_bin": {
            "type": "categorical", "choices": [128, 256, 512],
            "condition": {"param": "tree_method", "in": ["hist", "approx"]},
        },
    },
    "logistic": {
        "C": {"type": "float", "low": 0.0001, "high": 10.0, "log": True},
        # l1_ratio spans the full L2 → elasticnet → L1 spectrum: 0.0=pure L2,
        # 1.0=pure L1, intermediate=elasticnet mix. Replaces sklearn's deprecated
        # `penalty=` keyword (removed in sklearn 1.10). LogisticModel derives
        # solver from l1_ratio (lbfgs for 0, saga otherwise).
        "l1_ratio": {"type": "float", "low": 0.0, "high": 1.0},
    },
    "random_forest": {
        "n_estimators": {"type": "int", "low": 100, "high": 500, "step": 50},
        "max_depth": {"type": "categorical", "choices": [3, 4, 6, 8, 10, None]},
        "min_samples_split": {"type": "int", "low": 2, "high": 20},
        "min_samples_leaf": {"type": "int", "low": 5, "high": 50},
        "max_features": {"type": "categorical", "choices": ["sqrt", "log2", 0.3, 0.5, 0.7, 1.0]},
        "max_leaf_nodes": {"type": "categorical", "choices": [None, 50, 100, 200, 500]},
        "min_impurity_decrease": {"type": "float", "low": 0.0, "high": 0.01},
        "bootstrap": {"type": "categorical", "choices": [True, False]},
        "criterion": {"type": "categorical", "choices": ["gini", "log_loss"]},
        # max_samples controls per-tree sample fraction when bootstrap=True;
        # sklearn ignores when bootstrap=False. Meaningful bias/variance lever.
        "max_samples": {"type": "categorical", "choices": [None, 0.5, 0.7, 0.85, 1.0]},
        # ccp_alpha: cost-complexity pruning. Prunes subtrees whose contribution
        # to loss reduction doesn't justify their complexity. Different
        # regularization mechanism than min_samples_*/max_depth.
        "ccp_alpha": {"type": "float", "low": 0.0, "high": 0.05},
        # min_weight_fraction_leaf: leaf must have at least this fraction of
        # total sample weight. Different from min_samples_leaf when
        # sample_weight is non-uniform (runner passes time-decay weights).
        "min_weight_fraction_leaf": {"type": "float", "low": 0.0, "high": 0.05},
    },
    "neural_net": {
        "hidden_layers": {"type": "categorical", "choices": ["32", "64", "32-16", "64-32", "128-64", "256-128", "64-32-16", "128-64-32"]},
        "dropout": {"type": "float", "low": 0.1, "high": 0.5},
        "learning_rate": {"type": "float", "low": 0.0001, "high": 0.005, "log": True},
        "batch_size": {"type": "categorical", "choices": [256, 512, 1024, 2048]},
        "epochs": {"type": "int", "low": 15, "high": 50},
        "patience": {"type": "int", "low": 3, "high": 10},
        "normalization": {"type": "categorical", "choices": ["none", "batch", "layer"]},
        "label_smoothing": {"type": "float", "low": 0.0, "high": 0.1},
        "weight_decay": {"type": "float", "low": 0.0, "high": 0.01},
        "grad_clip_norm": {"type": "categorical", "choices": [None, 1.0, 5.0]},
        "lr_scheduler": {"type": "categorical", "choices": [None, "plateau"]},
        # lr_scheduler_factor / lr_scheduler_patience only have effect when
        # lr_scheduler="plateau" is sampled. factor = LR multiplier on plateau;
        # patience = epochs of no improvement before reducing. Conditional so
        # they aren't sampled (wasted) when lr_scheduler=None.
        "lr_scheduler_factor": {
            "type": "float", "low": 0.1, "high": 0.7,
            "condition": {"param": "lr_scheduler", "in": ["plateau"]},
        },
        "lr_scheduler_patience": {
            "type": "int", "low": 2, "high": 10,
            "condition": {"param": "lr_scheduler", "in": ["plateau"]},
        },
        # optimizer: "auto" preserves the original behavior (Adam if
        # weight_decay==0 else AdamW). The other choices override it.
        "optimizer": {"type": "categorical", "choices": ["auto", "adam", "adamw", "sgd_momentum", "radam", "nadam"]},
        # activation: hidden-layer activation function. ReLU is the historical
        # default; GELU is the modern default in transformers; SiLU (Swish)
        # shows up in vision/regression; LeakyReLU avoids dying-neuron issues.
        "activation": {"type": "categorical", "choices": ["relu", "gelu", "silu", "leaky_relu"]},
        # Fine-tune phase (applied after main training, on the most-recent
        # finetune_frac slice of training data with finetune_lr). finetune_frac=0
        # disables fine-tuning entirely — lets the tuner discover whether
        # fine-tuning helps or hurts on this config.
        "finetune_frac": {"type": "float", "low": 0.0, "high": 0.3},
        "finetune_lr": {"type": "float", "low": 0.00001, "high": 0.001, "log": True},
        "finetune_epochs": {"type": "int", "low": 10, "high": 50},
        "finetune_patience": {"type": "int", "low": 5, "high": 15},
    },
}


def suggest_params(
    trial: optuna.Trial,
    search_space: dict[str, dict[str, Any]],
    fixed: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Use an Optuna trial to suggest values for the params in the search space.

    A spec may carry a ``"condition"`` of the form
    ``{"param": <controller>, "in": [<values>]}``; that param is suggested only
    when the controller's value — taken from an already-suggested param, or from
    a pinned value in *fixed* — is in the allowed set. This keeps inert
    dimensions (e.g. ``max_bin`` under ``tree_method=exact``) from consuming TPE
    budget. Controllers must precede their dependents in the dict; a test guards
    that ordering.
    """
    fixed = fixed or {}
    params: dict[str, Any] = {}
    for name, spec in search_space.items():
        cond = spec.get("condition")
        if cond is not None:
            ctrl_val = params.get(cond["param"], fixed.get(cond["param"]))
            if ctrl_val not in cond["in"]:
                continue
        ptype = spec["type"]
        if ptype == "int":
            kwargs = {}
            if "step" in spec:
                kwargs["step"] = spec["step"]
            params[name] = trial.suggest_int(name, spec["low"], spec["high"], **kwargs)
        elif ptype == "float":
            params[name] = trial.suggest_float(
                name, spec["low"], spec["high"], log=spec.get("log", False)
            )
        elif ptype == "categorical":
            params[name] = trial.suggest_categorical(name, spec["choices"])
        else:
            raise ValueError(f"Unknown param type '{ptype}' for param '{name}'")
    return params


# Map string-encoded hidden_layers back to lists for neural_net models.
# Optuna's suggest_categorical only supports scalar types, not lists.
HIDDEN_LAYERS_MAP: dict[str, list[int]] = {
    "32": [32],
    "64": [64],
    "32-16": [32, 16],
    "64-32": [64, 32],
    "128-64": [128, 64],
    "256-128": [256, 128],
    "64-32-16": [64, 32, 16],
    "128-64-32": [128, 64, 32],
}


def _decode_params(params: dict[str, Any]) -> dict[str, Any]:
    """Decode string-encoded params back to their real types."""
    decoded = dict(params)
    if "hidden_layers" in decoded and isinstance(decoded["hidden_layers"], str):
        decoded["hidden_layers"] = HIDDEN_LAYERS_MAP[decoded["hidden_layers"]]
    # Expand the mutually-exclusive normalization choice into the two booleans
    # the model expects (batch_norm and layer_norm cannot both be True).
    if "normalization" in decoded:
        norm = decoded.pop("normalization")
        decoded["batch_norm"] = norm == "batch"
        decoded["layer_norm"] = norm == "layer"
    return decoded


def _param_combo_str(params: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(params.items()))


class HyperparamTuner:
    """Bayesian hyperparameter optimization via Optuna TPE."""

    def __init__(
        self,
        config_path: Path | str,
        search_space: dict[str, dict[str, Any]] | None = None,
        param_overrides: dict[str, Any] | None = None,
        metrics: list[str] | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        state_dir: Path | str | None = None,
        n_startup_trials: int | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.n_startup_trials = n_startup_trials
        # Per-trial xgb thread budget when running parallel trials (set in run()
        # when parallel_trials > 1); None = serial, use the config's own n_jobs.
        self._per_trial_n_jobs: int | None = None

        with open(self.config_path) as f:
            self.base_config = yaml.safe_load(f)

        self.is_iid = _is_iid_config(self.base_config)

        # Objective source. CLASSIFICATION: metrics.objective from the config —
        # the single source shared with early stopping + the pruner; no --metric,
        # no log_loss default, absent = hard error (never a silent fallback to an
        # arbitrary metric). A multi-element list = multi-objective (Pareto).
        # IID/projection keeps the `metrics=` (--metric) mechanism for now —
        # regression regime, no metrics block in serve_model configs (issue #96).
        if self.is_iid:
            self.metrics = metrics or ["mae"]
        else:
            objective = (self.base_config.get("metrics") or {}).get("objective")
            if not objective:
                raise ValueError(
                    f"tuning requires metrics.objective in {self.config_path} "
                    "(the metric(s) to optimize); none is set"
                )
            self.metrics = objective
        if self.is_iid:
            self.model_type = self.base_config["serve_model"].get("model_type", "xgboost")
        else:
            self.model_type = self.base_config["model"]["type"]

        # Tuning ignores `calibration:` in the config — calibration is a
        # deployment concern honored by `mvp model`, not an HP search concern.
        # Co-tuning HPs with Platt produces brittle winners that "game" the
        # calibrator. Warn the user so they don't expect the block to influence
        # tuning results.
        if not self.is_iid and self.base_config.get("calibration"):
            logger.warning(
                "config has a `calibration:` block — this is IGNORED during "
                "tuning. Calibration applies only at `mvp model` training "
                "time. Tuning evaluates raw predictor discrimination."
            )

        if search_space is not None:
            self.search_space = dict(search_space)
        elif self.model_type in DEFAULT_SEARCH_SPACES:
            self.search_space = dict(DEFAULT_SEARCH_SPACES[self.model_type])
        else:
            raise ValueError(
                f"No default search space for model type '{self.model_type}'"
                " — pass search_space explicitly"
            )

        # DART: rate_drop / skip_drop are only meaningful when booster="dart",
        # and DART trials are O(n_estimators²) which can hang at the default
        # n_estimators ceiling. So instead of putting them in the default
        # xgboost search space (which would force every routine XGB tune to
        # sample dart), they're conditionally added only when the user has
        # pinned `booster: dart` in the config — making DART an explicit
        # per-config opt-in. See models/prod_log_dart.yaml for the pattern.
        if (
            self.model_type == "xgboost"
            and (self.base_config.get("model") or {}).get("params", {}).get("booster") == "dart"
        ):
            self.search_space["rate_drop"] = {"type": "float", "low": 0.05, "high": 0.25}
            self.search_space["skip_drop"] = {"type": "float", "low": 0.0, "high": 0.5}

        # Early stopping owns the round count (it searches within es.ceiling), so
        # n_estimators is not a tunable HP under ES — drop it from the search to
        # avoid wasting a dead dimension the runner's ES factory would override.
        if (self.base_config.get("early_stopping") or {}).get("enabled"):
            self.search_space.pop("n_estimators", None)

        # MTL: extend the search space with per-target loss-weight dimensions
        # (one per configured aux target). Range widened to 0.01-5.0 after H38
        # set_margin tuned to 0.96 (right at the prior 1.0 ceiling).
        mtl_block = self.base_config.get("mtl")
        if mtl_block:
            # MTL trains vector-leaf multi-output trees, which XGBoost only
            # supports under tree_method=hist (gbtree.cc:205). Drop tree_method
            # from the search space so trials don't log an exact/approx value
            # that the model wrapper silently overrides to hist, and so the
            # TPE dimension isn't wasted on an ignored param.
            self.search_space.pop("tree_method", None)
            for aux in mtl_block.get("auxiliary_targets", []) or []:
                self.search_space[f"weight_{aux}"] = {
                    "type": "float", "low": 0.01, "high": 5.0, "log": True,
                }

        # Pin specific params, removing them from the search space
        self.pinned_params: dict[str, Any] = {}
        if param_overrides:
            for k, v in param_overrides.items():
                self.pinned_params[k] = v
                self.search_space.pop(k, None)

        # Set up Optuna storage
        state_dir_path = Path(state_dir) if state_dir else (get_data_root() / "tuning")
        state_dir_path.mkdir(parents=True, exist_ok=True)
        self.db_path = state_dir_path / f"{self.config_path.stem}.db"
        # ?timeout=30: SQLite busy-timeout (seconds) so concurrent trials under a
        # parallel study.optimize retry on a transient write lock rather than
        # raising OperationalError. No effect when running serially.
        storage = f"sqlite:///{self.db_path}?timeout=30"

        # Create or load study
        directions = [
            "maximize" if m in _MAXIMIZE_METRICS else "minimize"
            for m in self.metrics
        ]

        # Sampler and pruner share the same startup-trial threshold so the
        # pure-random exploration phase is fully protected from both TPE
        # modeling and pruning decisions. Default is 50 — well above
        # Optuna's TPESampler default of 10 — to give TPE a broader
        # foundation before it commits to a region. The search space is
        # ~13-17 dimensions (multivariate + conditional), so a thin random
        # seed leaves TPE's first "good set" density estimate too sparse;
        # this is compounded now that the objective (metrics.objective) is
        # noisier per fold than log_loss. 50 also keeps early noisy-fold
        # metrics from feeding pruning decisions.
        # CAVEAT: the pruner config is not persisted in the SQLite study.
        # Resuming a study constructs a fresh pruner — if the original run
        # used a non-default --n-startup-trials, the resume invocation MUST
        # pass the same value or the new pruner will fire earlier than
        # intended for trials added in that resume session.
        startup_trials = (
            self.n_startup_trials if self.n_startup_trials is not None else 50
        )
        # multivariate=True models HP interactions (tree HPs are correlated, so
        # the univariate default leaves signal on the table); group=True lets the
        # multivariate TPE handle the conditional search space — trials with
        # different active params — via Gibbs sampling. Both are flagged
        # experimental in optuna 4.8 but stable in practice; the warning fires
        # once at construction, so suppress it there to keep tune logs clean.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ExperimentalWarning)
            sampler = optuna.samplers.TPESampler(
                n_startup_trials=startup_trials,
                multivariate=True,
                group=True,
            )

        # MedianPruner kills trials whose fold-k tuning objective
        # (metrics.objective) is worse than the median of completed trials
        # at the same fold step. warmup=2 means
        # pruning can only fire from fold 2 onward; fold 0 and fold 1
        # metrics are too noisy on tabular CV to drive kills. See
        # scripts/analyze_fold_predictiveness.py for the empirical
        # validation (zero top-K trials killed at warmup=2 across 50 trials).
        pruner = optuna.pruners.MedianPruner(
            n_startup_trials=startup_trials,
            n_warmup_steps=2,
            n_min_trials=10,
            interval_steps=1,
        )

        self.study = optuna.create_study(
            study_name=self.config_path.stem,
            storage=storage,
            directions=directions,
            load_if_exists=True,
            sampler=sampler,
            pruner=pruner,
        )

        if self.pinned_params:
            self.study.set_user_attr("pinned_params", self.pinned_params)

        # Enqueue baseline trial from config params
        self._enqueue_baseline()

    def _get_base_params(self) -> dict[str, Any]:
        if self.is_iid:
            return self.base_config.get("serve_model", {}).get("params", {})
        return self.base_config.get("model", {}).get("params", {})

    def _enqueue_baseline(self) -> None:
        """Enqueue the current config params as the first trial (skip on resume)."""
        if len(self.study.trials) > 0:
            return  # Study already has trials — don't re-enqueue baseline
        base_params = self._get_base_params()
        baseline = {}
        for k in self.search_space:
            if k == "normalization":
                # Encode the config's two booleans into the single search-space
                # choice (inverse of the decode in _decode_params).
                if base_params.get("batch_norm"):
                    baseline[k] = "batch"
                elif base_params.get("layer_norm"):
                    baseline[k] = "layer"
                else:
                    baseline[k] = "none"
            elif k in base_params:
                baseline[k] = base_params[k]
        # Only enqueue if we have values for all search space params
        if len(baseline) == len(self.search_space):
            self.study.enqueue_trial(baseline)

    def _objective(self, trial: optuna.Trial) -> float | tuple[float, ...]:
        """Optuna objective: suggest params, run experiment, return metric(s).

        Raises optuna.TrialPruned if the runner's per-fold pruning check
        fires mid-trial. That exception propagates up to Optuna's optimize
        loop, which records the trial in PRUNED state."""
        params = suggest_params(trial, self.search_space, fixed=self.pinned_params)
        params.update(self.pinned_params)
        params = _decode_params(params)

        # Pass `trial` so the runner can report per-fold log_loss and
        # consult the pruner at each tuning-fold boundary.
        result = self._run_one(params, trial=trial)

        # Mark trials as raw-mode so `tune-review` can distinguish post-refactor
        # studies (raw discrimination) from legacy studies (Platt-calibrated).
        # IID / projection trials don't go through the Platt path either way,
        # but flagging uniformly keeps the leaderboard logic simple.
        trial.set_user_attr("_tuning_mode", "raw")

        # Store all metrics as user attrs for review
        for metric_name, metric_value in result["metrics"].items():
            trial.set_user_attr(metric_name, metric_value)

        # Holdout metrics (last-fold OOS check) — used by tune-review to
        # re-rank trials by the honest metric, not the tuning-set metric.
        if result.get("holdout_metrics"):
            for metric_name, metric_value in result["holdout_metrics"].items():
                trial.set_user_attr(f"holdout_{metric_name}", metric_value)

        # Inner CV diagnostics so we can confirm the noise-reduction layer is
        # actually firing (and didn't silently fall back to single-fit per fold).
        if result.get("inner_cv_folds"):
            trial.set_user_attr("inner_cv_folds", result["inner_cv_folds"])
        if result.get("inner_fold_count_per_outer") is not None:
            trial.set_user_attr(
                "inner_fold_count_per_outer",
                result["inner_fold_count_per_outer"],
            )

        # Per-fold metrics — needed for retrospective analyses (e.g., is
        # fold-1 log_loss predictive of the mean across folds, which informs
        # whether per-fold pruning would be safe to enable).
        if result.get("fold_metrics"):
            trial.set_user_attr("fold_metrics", result["fold_metrics"])
        if result.get("holdout_fold_metrics"):
            trial.set_user_attr("holdout_fold_metrics", result["holdout_fold_metrics"])

        trial.set_user_attr("duration_s", result["duration_s"])

        if len(self.metrics) == 1:
            return result["metrics"][self.metrics[0]]
        return tuple(result["metrics"][m] for m in self.metrics)

    def _run_one(
        self, params: dict[str, Any], trial: optuna.Trial | None = None,
    ) -> dict[str, Any]:
        """Run a single param combination through the appropriate runner.

        When `trial` is provided, the underlying runner reports the per-fold
        tuning objective (metrics.objective) and may raise optuna.TrialPruned
        mid-run.
        """
        config = dict(self.base_config)
        if self.is_iid:
            config["serve_model"] = dict(config["serve_model"])
            base_params = dict(config["serve_model"].get("params") or {})
            base_params.update(params)
            if self._per_trial_n_jobs is not None:
                base_params["n_jobs"] = self._per_trial_n_jobs
            config["serve_model"]["params"] = base_params
        else:
            config["model"] = dict(config["model"])
            base_params = dict(config["model"].get("params") or {})
            base_params.update(params)
            # Per-trial thread split for parallel tuning. Injected into the
            # transient temp-config only — NOT into trial.params or
            # result["params"], so the persisted winning config keeps its own
            # n_jobs. Spread last in models.py (**resolved), so it wins over the
            # config value and the --n-jobs override for the duration of a fit.
            if self._per_trial_n_jobs is not None:
                base_params["n_jobs"] = self._per_trial_n_jobs
            config["model"]["params"] = base_params

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as f:
            yaml.dump(config, f, default_flow_style=False)
            temp_path = Path(f.name)

        try:
            t0 = time.perf_counter()
            # Per-fold runner/engine logging is quieted once in run() — NOT
            # per-trial here — so concurrent trials (parallel_trials>1) don't
            # race on the shared logger levels.
            if self.is_iid:
                from mvp.projection.iid.runner import IIDProjectionRunner

                runner = IIDProjectionRunner(
                    config_path=temp_path,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                    run_name=f"tune_{self.config_path.stem}",
                    log_to_mlflow=False,
                )
            elif self.model_type in _PROJECTION_MODEL_TYPES:
                from mvp.projection.runner import ProjectionRunner

                runner = ProjectionRunner(
                    config_path=temp_path,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                    run_name=f"tune_{self.config_path.stem}",
                    log_to_mlflow=False,
                )
            else:
                from mvp.model.runner import ExperimentRunner

                # calibrate=False: HP search optimizes raw discrimination.
                # Calibration is a deployment concern handled by `mvp model`
                # (ProductionPredictor), not an HP-tuning concern. The
                # projection / IID runners above don't fit Platt today so
                # they don't need an analogous flag.
                runner = ExperimentRunner(
                    config_path=temp_path,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                    run_name=f"tune_{self.config_path.stem}",
                    log_to_mlflow=False,
                    holdout_folds=1,
                    inner_cv_folds=4,
                    calibrate=False,
                )
            # IID / projection runners don't currently support pruning;
            # only ExperimentRunner threads `trial` through. Pass it where
            # accepted, ignore where not.
            if self.is_iid or self.model_type in _PROJECTION_MODEL_TYPES:
                result = runner.run()
            else:
                result = runner.run(trial=trial)
            metrics = dict(result["metrics"])
            holdout_metrics = (
                dict(result["holdout_metrics"])
                if result.get("holdout_metrics") is not None
                else None
            )
            inner_cv_folds_used = result.get("inner_cv_folds") or 0
            inner_fold_count_per_outer = (
                list(result["inner_fold_count_per_outer"])
                if result.get("inner_fold_count_per_outer") is not None
                else None
            )
            fold_metrics = (
                [dict(f) for f in result["fold_metrics"]]
                if result.get("fold_metrics") is not None
                else None
            )
            holdout_fold_metrics = (
                [dict(f) for f in result["holdout_fold_metrics"]]
                if result.get("holdout_fold_metrics") is not None
                else None
            )
            duration = time.perf_counter() - t0

            # Drop large per-trial state (fold predictions, diagnostics, mlflow
            # buffers via runner) before returning so memory doesn't accumulate
            # across Optuna trials.
            del result
            del runner
            gc.collect()

            return {
                "params": params,
                "metrics": metrics,
                "holdout_metrics": holdout_metrics,
                "inner_cv_folds": inner_cv_folds_used,
                "inner_fold_count_per_outer": inner_fold_count_per_outer,
                "fold_metrics": fold_metrics,
                "holdout_fold_metrics": holdout_fold_metrics,
                "duration_s": round(duration, 1),
            }
        finally:
            temp_path.unlink(missing_ok=True)

    def run(
        self, n_trials: int, verbose: bool = True, parallel_trials: int = 1,
    ) -> optuna.Study:
        """Run Bayesian optimization for n_trials.

        parallel_trials (K): run K trials concurrently via Optuna's thread pool.
        Each concurrent trial's xgb gets ``T // K`` threads (T = the config's
        n_jobs, else the cpu-2 default), so the total thread budget is unchanged
        — this trades idle threads (xgb scales sub-linearly past a knee) for more
        in-flight trials. K is capped at 2 by the CLI (K>=3 would need
        constant_liar, which conflicts with the group=True TPE sampler). K=1 =
        serial (default, unchanged behavior).
        """
        completed = sum(
            1 for t in self.study.trials
            if t.state == optuna.trial.TrialState.COMPLETE
        )
        total = len(self.study.trials)
        zombie = total - completed
        logger.info(
            "Tuning %s (%s): %d trials requested, %d completed "
            "(%d total in study, %d zombie/incomplete)",
            self.config_path.stem, self.model_type, n_trials,
            completed, total, zombie,
        )

        # Suppress Optuna's own trial-level logging; we log ourselves
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        callbacks = []
        if verbose:
            callbacks.append(self._log_trial_callback)

        # Quiet the per-fold runner/engine logs for the whole tune. Done ONCE
        # here (not per-trial in _run_one) so concurrent trials under
        # parallel_trials>1 don't race on these shared logger levels. The
        # early_stopping logger is intentionally left alone so its per-fold
        # best_iteration lines still show.
        _quiet = [
            logging.getLogger(name) for name in (
                "mvp.model.runner", "mvp.model.engine",
                "mvp.projection.runner", "mvp.projection.iid.runner",
            )
        ]
        _prev_levels = [lg.level for lg in _quiet]
        for lg in _quiet:
            lg.setLevel(logging.WARNING)
        try:
            if parallel_trials > 1:
                # Split the config's thread budget across the K concurrent trials.
                budget = self._get_base_params().get("n_jobs")
                if budget is None:
                    budget = _default_n_jobs()
                self._per_trial_n_jobs = max(1, int(budget) // parallel_trials)
                logger.info(
                    "Parallel trials: K=%d, per-trial n_jobs=%d (budget %s)",
                    parallel_trials, self._per_trial_n_jobs, budget,
                )
                # Warm the feature/transform cache with ONE synchronous trial
                # before fanning out, so K cold-start trials don't each recompute
                # the whole-matrix transform self-join concurrently.
                self.study.optimize(self._objective, n_trials=1, callbacks=callbacks)
                remaining = max(0, n_trials - 1)
                if remaining:
                    self.study.optimize(
                        self._objective,
                        n_trials=remaining,
                        n_jobs=parallel_trials,
                        callbacks=callbacks,
                    )
            else:
                self.study.optimize(
                    self._objective,
                    n_trials=n_trials,
                    callbacks=callbacks,
                )
        finally:
            for lg, lvl in zip(_quiet, _prev_levels):
                lg.setLevel(lvl)

        logger.info(
            "Tuning complete: %d total trials in %s",
            len(self.study.trials), self.db_path,
        )
        return self.study

    def _log_trial_callback(
        self, study: optuna.Study, trial: optuna.trial.FrozenTrial
    ) -> None:
        """Log each trial. Pruned trials get a one-line summary since their
        user_attrs (which `_objective` sets after `_run_one` returns) are
        empty — the prune raised before that code ran."""
        if trial.state == optuna.trial.TrialState.PRUNED:
            # Pruned trials carry no user_attrs; report which fold step
            # killed them (intermediate_values is set by Optuna from the
            # runner's trial.report() calls before the prune).
            last_step = max(trial.intermediate_values) if trial.intermediate_values else "?"
            logger.info(
                "Trial %d: PRUNED at step %s | %s",
                trial.number, last_step, _param_combo_str(trial.params),
            )
            return
        metrics_str = ", ".join(
            f"{m}={trial.user_attrs.get(m, 'N/A'):.4f}"
            if isinstance(trial.user_attrs.get(m), float) else f"{m}=N/A"
            for m in self.metrics
        )
        duration = trial.user_attrs.get("duration_s", "?")
        logger.info(
            "Trial %d: %s | %s | %.1fs",
            trial.number,
            _param_combo_str(trial.params),
            metrics_str,
            duration if isinstance(duration, float) else 0.0,
        )
