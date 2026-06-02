"""Model hyperparameter tuning via Optuna Bayesian optimization."""

import gc
import logging
import tempfile
import time
from pathlib import Path
from typing import Any

import optuna
import yaml

from mvp.common.base_job import get_data_root
from mvp.projection.iid.metric_registry import METRICS as _IID_METRICS

logger = logging.getLogger(__name__)

_PROJECTION_MODEL_TYPES = {"xgb_regressor", "linear", "ridge"}

# Manually-tracked maximize metrics that aren't in the IID registry, plus
# point-grain variants (re-emitted with a "point_" prefix by the score-state
# serve model) for any registry entry whose direction is "maximize".
_MAXIMIZE_METRICS = (
    {"accuracy", "roc_auc", "r_squared"}
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
        "colsample_bynode": {"type": "float", "low": 0.5, "high": 1.0},
        "gamma": {"type": "float", "low": 0.0, "high": 10.0},
        "reg_alpha": {"type": "float", "low": 0.0, "high": 1.0},
        "reg_lambda": {"type": "float", "low": 0.1, "high": 10.0, "log": True},
        "max_delta_step": {"type": "int", "low": 0, "high": 5},
        "scale_pos_weight": {"type": "float", "low": 0.9, "high": 1.1},
    },
    "logistic": {
        "C": {"type": "float", "low": 0.0001, "high": 10.0, "log": True},
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
    },
    "neural_net": {
        "hidden_layers": {"type": "categorical", "choices": ["32", "64", "32-16", "64-32", "128-64", "256-128", "64-32-16", "128-64-32"]},
        "dropout": {"type": "float", "low": 0.1, "high": 0.5},
        "learning_rate": {"type": "float", "low": 0.0001, "high": 0.005, "log": True},
        "batch_size": {"type": "categorical", "choices": [256, 512, 1024, 2048]},
        "epochs": {"type": "int", "low": 15, "high": 50},
        "patience": {"type": "int", "low": 3, "high": 10},
        "batch_norm": {"type": "categorical", "choices": [True, False]},
        "label_smoothing": {"type": "float", "low": 0.0, "high": 0.1},
        "weight_decay": {"type": "float", "low": 0.0, "high": 0.01},
        "grad_clip_norm": {"type": "categorical", "choices": [None, 1.0, 5.0]},
        "layer_norm": {"type": "categorical", "choices": [True, False]},
        "lr_scheduler": {"type": "categorical", "choices": [None, "plateau"]},
    },
}


def suggest_params(
    trial: optuna.Trial, search_space: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Use an Optuna trial to suggest values for all params in the search space."""
    params: dict[str, Any] = {}
    for name, spec in search_space.items():
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
    ) -> None:
        self.config_path = Path(config_path)
        self.metrics = metrics or ["log_loss"]
        self.matches_path = matches_path
        self.cache_dir = cache_dir

        with open(self.config_path) as f:
            self.base_config = yaml.safe_load(f)

        self.is_iid = _is_iid_config(self.base_config)
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

        # MTL: extend the search space with per-target loss-weight dimensions
        # (one per configured aux target). Log-uniform 0.01-1.0 covers three
        # decades. Primary weight is fixed at 1.0 — only relative weights
        # matter, so tuning the primary alongside aux would just be a global
        # scale knob with no effect on the optimum.
        mtl_block = self.base_config.get("mtl")
        if mtl_block:
            for aux in mtl_block.get("auxiliary_targets", []) or []:
                self.search_space[f"weight_{aux}"] = {
                    "type": "float", "low": 0.01, "high": 1.0, "log": True,
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
        storage = f"sqlite:///{self.db_path}"

        # Create or load study
        directions = [
            "maximize" if m in _MAXIMIZE_METRICS else "minimize"
            for m in self.metrics
        ]

        self.study = optuna.create_study(
            study_name=self.config_path.stem,
            storage=storage,
            directions=directions,
            load_if_exists=True,
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
            if k in base_params:
                baseline[k] = base_params[k]
        # Only enqueue if we have values for all search space params
        if len(baseline) == len(self.search_space):
            self.study.enqueue_trial(baseline)

    def _objective(self, trial: optuna.Trial) -> float | tuple[float, ...]:
        """Optuna objective: suggest params, run experiment, return metric(s)."""
        params = suggest_params(trial, self.search_space)
        params.update(self.pinned_params)
        params = _decode_params(params)

        result = self._run_one(params)

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

        trial.set_user_attr("duration_s", result["duration_s"])

        if len(self.metrics) == 1:
            return result["metrics"][self.metrics[0]]
        return tuple(result["metrics"][m] for m in self.metrics)

    def _run_one(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run a single param combination through the appropriate runner."""
        config = dict(self.base_config)
        if self.is_iid:
            config["serve_model"] = dict(config["serve_model"])
            base_params = dict(config["serve_model"].get("params") or {})
            base_params.update(params)
            config["serve_model"]["params"] = base_params
        else:
            config["model"] = dict(config["model"])
            base_params = dict(config["model"].get("params") or {})
            base_params.update(params)
            config["model"]["params"] = base_params

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as f:
            yaml.dump(config, f, default_flow_style=False)
            temp_path = Path(f.name)

        try:
            t0 = time.perf_counter()
            # Suppress per-fold logging during tuning
            runner_logger = logging.getLogger("mvp.model.runner")
            engine_logger = logging.getLogger("mvp.model.engine")
            proj_logger = logging.getLogger("mvp.projection.runner")
            iid_logger = logging.getLogger("mvp.projection.iid.runner")
            prev_runner = runner_logger.level
            prev_engine = engine_logger.level
            prev_proj = proj_logger.level
            prev_iid = iid_logger.level
            runner_logger.setLevel(logging.WARNING)
            engine_logger.setLevel(logging.WARNING)
            proj_logger.setLevel(logging.WARNING)
            iid_logger.setLevel(logging.WARNING)
            try:
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
                result = runner.run()
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
            finally:
                runner_logger.setLevel(prev_runner)
                engine_logger.setLevel(prev_engine)
                proj_logger.setLevel(prev_proj)
                iid_logger.setLevel(prev_iid)
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
                "duration_s": round(duration, 1),
            }
        finally:
            temp_path.unlink(missing_ok=True)

    def run(self, n_trials: int, verbose: bool = True) -> optuna.Study:
        """Run Bayesian optimization for n_trials."""
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

        self.study.optimize(
            self._objective,
            n_trials=n_trials,
            callbacks=callbacks,
        )

        logger.info(
            "Tuning complete: %d total trials in %s",
            len(self.study.trials), self.db_path,
        )
        return self.study

    def _log_trial_callback(
        self, study: optuna.Study, trial: optuna.trial.FrozenTrial
    ) -> None:
        """Log each completed trial."""
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
