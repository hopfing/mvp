"""Model hyperparameter tuning via Optuna Bayesian optimization."""

import gc
import logging
import sys
import tempfile
import threading
import time
import warnings
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path
from typing import Any

import optuna
import yaml
from optuna.exceptions import ExperimentalWarning
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from mvp.common.base_job import get_data_root
from mvp.model.metrics import (
    CALIBRATION_INVARIANT_METRICS,
    MAXIMIZE_METRICS as _MODEL_MAXIMIZE_METRICS,
)
from mvp.model.models import _default_n_jobs
from mvp.projection.iid.metric_registry import METRICS as _IID_METRICS

logger = logging.getLogger(__name__)

_PROJECTION_MODEL_TYPES = {"xgb_regressor", "linear", "ridge"}

# Marker stamped on classification studies built under the forward-aligned (v2)
# objective. Studies from the legacy within-window inner-CV objective lack it (or
# carry a different value) and are refused for resume — the two objectives are
# numerically incomparable. Bump this string on any future objective-frame change.
_OBJECTIVE_FRAME = "forward_v2"
# Calibrated-frame search: probability-scale objectives search on the calibrated
# out-of-fold metric, not raw. Numerically distinct from the raw-frame objective,
# so it carries its own marker and won't resume against a raw-frame study (or vice
# versa). Bump on any future calibrated-objective-frame change.
_OBJECTIVE_FRAME_CAL = "forward_cal_v1"

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


# Trailing forward folds held search-blind for selection (classification only).
_DEFAULT_OUTER_FOLDS = 4

# `serve_model` param blocks a study can search, mirroring the split
# `ServeModelConfig` already makes. `params` drives the two win branches (one
# implementation separated by a fit-time row filter, similar scale, same grain);
# `first_in_params` drives the mixing weight, which is a different grain, a
# different target, and ~45x fewer rows.
_SERVE_BLOCKS: frozenset[str] = frozenset({"params", "first_in_params"})


# Joint two-level search. The two blocks are not separable: the objective only
# exists on the composed `p = f*w1 + (1-f)*w2`, so a `params` trial can only be
# scored against SOME `first_in_params`, and vice versa. A sequential ladder
# therefore returns the end of an arbitrary path rather than the best pair. One
# study samples both and scores the composite once.
WIN_PREFIX = "win_"
FIRST_IN_PREFIX = "fi_"

# Knobs that cannot do anything on the first_in arm, so spending a TPE dimension
# on them is pure cost against an objective the study already struggles to
# separate (see the pruning note in `run`).
#   scale_pos_weight / max_delta_step — classifier knobs. `first_in` is an
#     XGBRegressor on a rate (two_level_serve_model.py:301-311); there is no
#     class balance for either to act on.
#   colsample_bylevel / bynode — three multiplicative feature samplers stacked
#     over ~6 candidate columns cannot differentiate. `colsample_bytree` stays.
_FIRST_IN_DROP = frozenset(
    {"scale_pos_weight", "max_delta_step", "colsample_bylevel", "colsample_bynode"}
)
# Depth past the feature count has nothing left to split on.
_FIRST_IN_NARROW: dict[str, dict[str, Any]] = {
    "max_depth": {"type": "int", "low": 3, "high": 6},
}


def _prefixed_space(
    space: dict[str, dict[str, Any]], prefix: str,
) -> dict[str, dict[str, Any]]:
    """Namespace a search space, conditions included.

    `condition.param` names a SIBLING key (`tree_method` gates `grow_policy`,
    `max_bin`, `colsample_bynode`), and `_suggest_params` resolves it by exact
    name. Prefixing the keys without prefixing the references would leave every
    conditional looking up a name that no longer exists — it would silently read
    None and skip the param rather than raise.
    """
    out: dict[str, dict[str, Any]] = {}
    for name, spec in space.items():
        s = dict(spec)
        cond = s.get("condition")
        if cond is not None:
            s["condition"] = {**cond, "param": f"{prefix}{cond['param']}"}
        out[f"{prefix}{name}"] = s
    return out


def two_level_joint_space(
    base: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """One flat space covering both serve_model param blocks."""
    first_in = {k: v for k, v in base.items() if k not in _FIRST_IN_DROP}
    first_in.update(
        {k: v for k, v in _FIRST_IN_NARROW.items() if k in first_in}
    )
    return {
        **_prefixed_space(base, WIN_PREFIX),
        **_prefixed_space(first_in, FIRST_IN_PREFIX),
    }


def split_joint_params(flat: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """A joint trial's flat params back into (win-branch, first_in) blocks.

    STRICT. An unprefixed key is not a key with no home — it is proof the caller
    is reading a FLAT-namespace study as a joint one, and the only two things
    that can follow are wrong. Dropping it silently was the live bug: a flat
    study's bare `max_depth` matched neither prefix, both blocks came back empty,
    the merge onto the base config was a no-op, and `iid-sweep` wrote N
    byte-identical configs that all hashed to one fingerprint and overwrote each
    other — N trials evaluated, none of them exercised, no error anywhere.
    """
    win: dict[str, Any] = {}
    first_in: dict[str, Any] = {}
    foreign: list[str] = []
    for k, v in flat.items():
        if k.startswith(WIN_PREFIX):
            win[k[len(WIN_PREFIX):]] = v
        elif k.startswith(FIRST_IN_PREFIX):
            first_in[k[len(FIRST_IN_PREFIX):]] = v
        else:
            foreign.append(k)
    if foreign:
        raise ValueError(
            f"split_joint_params: {sorted(foreign)} carry neither the "
            f"'{WIN_PREFIX}' nor the '{FIRST_IN_PREFIX}' prefix, so these params "
            "were not written by a joint two-level search. Whatever produced "
            f"them is in the '{NAMESPACE_FLAT}' namespace and its params belong "
            "in one serve_model block, unsplit."
        )
    return win, first_in


# The key namespace a study's trials are encoded in. Stamped on the study rather
# than re-derived by each reader: jointness is a property of how the trials
# ALREADY on disk were written, and every input to that decision (the config's
# first_in features, the caller's --serve-block) can change afterwards.
PARAM_NAMESPACE_ATTR = "param_namespace"
NAMESPACE_JOINT = "two_level_joint"
NAMESPACE_FLAT = "flat"


def is_joint_two_level(
    serve_model: dict[str, Any] | None, serve_block: str | None,
) -> bool:
    """Does a search over this config sample BOTH serve_model param blocks?

    ONE definition, called by the writer (`HyperparamTuner`) and — only to stamp
    or validate the study attr — never by a reader deciding how to interpret
    trials that already exist. A second copy of this predicate is exactly what
    broke `iid-sweep`: it tested two of the three conditions, so a two-level
    config whose first_in arm is intercept-only read as joint on the sweep side
    and flat on the tuner side.

    The third condition is the one that got dropped. A first_in arm with no
    features never fits a model (`FirstServeInModel` leaves `_model = None` and
    returns its training base rate), so `first_in_params` is dead config and
    there is only one searchable block — flat, like a single-level model.
    """
    if serve_block is not None:
        return False
    sm = serve_model or {}
    if sm.get("type") != "two_level":
        return False
    return bool(
        sm.get("first_in_match_features") or sm.get("first_in_point_features")
    )


def study_param_namespace(study: "optuna.Study") -> str:
    """Which namespace THIS study's trials are in, from the study itself.

    Prefers the stamp; falls back to reading the trials, so studies written
    before the stamp existed are still interpretable without a re-tune. The
    trials are the ground truth here — the stamp only records what the writer
    believed at creation.
    """
    stamped = study.user_attrs.get(PARAM_NAMESPACE_ATTR)
    if stamped is not None:
        return stamped
    for t in study.trials:
        if not t.params:
            continue
        if any(
            k.startswith((WIN_PREFIX, FIRST_IN_PREFIX)) for k in t.params
        ):
            return NAMESPACE_JOINT
        return NAMESPACE_FLAT
    return NAMESPACE_FLAT


def tuning_study_key(config_stem: str, serve_block: str | None = None) -> str:
    """Optuna study name for a config, or for one block of it.

    ONE definition, imported by both the writer (`HyperparamTuner`) and the
    readers (`mvp tune-review`, `sweep.py`). Two copies would let a reader open
    a study the writer never wrote — or worse, the wrong block's, since a
    single-block study suggests bare param names either way.

    `None` is the normal case and keeps the bare stem: a classification or
    single-level config, or a two-level config searched JOINTLY.

    Every override is suffixed, `params` included. A joint study over the same
    config uses PREFIXED param names (`win_max_depth`), so letting a bare-name
    `--serve-block params` study share the stem would resume one as the other
    and reinterpret every value.
    """
    if serve_block is None:
        return config_stem
    return f"{config_stem}__{serve_block}"


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
        # histograms; exact evaluates every split value. approx (quantile
        # sketches) was dropped after measurement: it was the slowest method
        # and never reached the best held-out result across the log_ds2412
        # studies, while exact sometimes does. Narrowing the categorical breaks
        # Optuna resume of studies created with the 3-choice space, so this is
        # a deliberate fresh-study line, not a retrofit. Placed before the
        # params that depend on it (colsample_bynode, grow_policy, max_bin) so
        # the conditional sampler sees its controller first.
        "tree_method": {"type": "categorical", "choices": ["hist", "exact"]},
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
    "lightgbm": {
        # num_leaves: the primary capacity control for leaf-wise (best-first)
        # growth — LightGBM does not grow depth-first, so this, not max_depth,
        # is the binding tree-shape knob. Ceiling 127 ≈ depth-7; floor 15 avoids
        # degenerate near-stumps. This is the main overfit guard on the
        # small-to-medium per-fold n here.
        "num_leaves": {"type": "int", "low": 15, "high": 127},
        # max_depth: secondary guard. -1 = unbounded (idiomatic leaf-wise, with
        # num_leaves the sole shape constraint); the positive options let the
        # tuner discover whether an added depth bound helps generalization.
        # Categorical because the int sampler can't express "unbounded".
        "max_depth": {"type": "categorical", "choices": [-1, 6, 8, 10, 12]},
        "learning_rate": {"type": "float", "low": 0.01, "high": 0.15, "log": True},
        "n_estimators": {"type": "int", "low": 100, "high": 1000, "step": 50},
        # min_child_samples: minimum data points per leaf — the critical
        # leaf-wise overfit guard on thin folds (challenger slices can be
        # sparse). LightGBM's default is 20; the range straddles it.
        "min_child_samples": {"type": "int", "low": 10, "high": 60},
        # subsample: row-sampling fraction per boosting round. Only takes effect
        # because LightGBMModel fixes subsample_freq=1 in its constructor —
        # LightGBM otherwise ignores subsample entirely.
        "subsample": {"type": "float", "low": 0.5, "high": 1.0},
        # colsample_bytree: per-tree feature sampling (LightGBM's
        # feature_fraction). LightGBM has no bynode/bylevel analog.
        "colsample_bytree": {"type": "float", "low": 0.5, "high": 1.0},
        "reg_alpha": {"type": "float", "low": 0.0, "high": 1.0},
        "reg_lambda": {"type": "float", "low": 0.1, "high": 10.0, "log": True},
        # min_split_gain: minimum loss reduction required to make a split —
        # LightGBM's analog of XGBoost's gamma.
        "min_split_gain": {"type": "float", "low": 0.0, "high": 5.0},
        # scale_pos_weight: near-inert (the target is structurally ~50/50, one
        # winner per match) but kept for parity with the xgboost space.
        "scale_pos_weight": {"type": "float", "low": 0.9, "high": 1.1},
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

    # Forward-aligned selection is winner's-curse-exposed when the search rests on
    # too few forward folds. Below _MIN_TUNING_FOLDS we warn but proceed — the
    # operator chose the outer_folds split, and a 2-4 fold tune is thin but valid.
    # Below _HARD_MIN_TUNING_FOLDS we hard-stop: with <2 folds the calibrated-frame
    # objective can't be computed, so the study would silently search raw while
    # stamped calibrated (see _objective's fallback), which is a correctness bug,
    # not a judgment call.
    _MIN_TUNING_FOLDS = 5
    _HARD_MIN_TUNING_FOLDS = 2

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
        outer_folds: int | None = None,
        seed: int | None = None,
        serve_block: str | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        # `serve_block` is an OVERRIDE, not the normal path. Default None means
        # "search everything this config has": one block for a classification or
        # single-level config, and BOTH blocks jointly for a two-level one.
        #
        # Joint, because the two blocks are not separable. A two-level model is
        # three fits — win branches on ~3.0M and ~1.9M point rows, `first_in` on
        # ~68k match-grain rows against a different target — and the objective
        # exists only on the composed `p = f*w1 + (1-f)*w2`. So a `params` trial
        # can only be scored against SOME `first_in_params` and vice versa: the
        # fits are independent (disjoint rows), the OPTIMA are not. Tuning them
        # in sequence returns the end of an arbitrary path — best `params` given
        # whatever `f` happened to be sitting there, then best `f` given that —
        # not the best pair. Nothing in the repo measures that interaction as
        # weak, and a nonlinear metric over a weighted composite has no reason
        # to be additive in the two blocks.
        #
        # The override survives for a deliberate single-block re-search once the
        # joint study has run. It must never be the default: silently searching
        # `params` alone leaves `first_in` inheriting whatever `params` wins
        # (serve_model.py:140), which reads as a tuned model when a third of it
        # never was.
        self._requested_block = serve_block
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.n_startup_trials = n_startup_trials
        outer_folds_explicit = outer_folds is not None
        if outer_folds is None:
            outer_folds = _DEFAULT_OUTER_FOLDS
        if outer_folds < 1:
            raise ValueError(f"outer_folds must be >= 1, got {outer_folds}")
        # Forward-aligned tuning: the objective is the metric over the inner
        # (tuning) folds' TRUE forward windows; `outer_folds` trailing folds are
        # held search-blind for selection. The legacy within-window inner-CV
        # objective is intentionally unreachable — it optimized interpolation, not
        # forward prediction.
        self.outer_folds = outer_folds
        self.seed = seed
        # Per-trial xgb thread budget when running parallel trials (set in run()
        # when parallel_trials > 1); None = serial, use the config's own n_jobs.
        self._per_trial_n_jobs: int | None = None
        # One-shot guard for the calibrated-frame fallback warning (below).
        self._cal_fallback_warned = False
        # Progress-bar state (set in run() when the bar is used; see
        # _progress_trial_callback). The lock guards the bar because
        # parallel_trials > 1 fires the callback from Optuna worker threads.
        self._bar: Any | None = None
        self._bar_lock = threading.Lock()
        # Best is seeded from the study (all-time, not session), so mark whether
        # this session is the one that set it — an unstarred value that never
        # moves is the search failing to beat its own incumbent.
        self._bar_best: float | None = None
        self._bar_best_is_session = False
        self._bar_pruned = 0

        with open(self.config_path) as f:
            self.base_config = yaml.safe_load(f)

        self.is_iid = _is_iid_config(self.base_config)

        # `outer_folds` is never passed to IIDProjectionRunner — there is no
        # search-blind block for IID, and the objective is the mean over ALL
        # folds. Accepting the flag silently implied a holdout that doesn't
        # exist, so say so instead of ignoring it.
        if self.is_iid and outer_folds_explicit:
            raise ValueError(
                "--outer-folds is not supported for IID/projection tuning: there "
                "is no search-blind outer block, the objective is the mean over "
                "all validation folds. Drop the flag."
            )

        # Fail fast, before any Optuna storage/study side effects, if the span
        # can't feed a trustworthy forward-aligned search.
        self._preflight_fold_check()

        if self.is_iid:
            self.model_type = self.base_config["serve_model"].get("model_type", "xgboost")
        else:
            self.model_type = self.base_config["model"]["type"]

        stype = (
            self.base_config.get("serve_model", {}).get("type")
            if self.is_iid else None
        )
        self.is_two_level = stype == "two_level"
        # A first_in arm with no features is INTERCEPT-ONLY: FirstServeInModel
        # sets `_model = None` and returns its training base rate
        # (two_level_serve_model.py:193-199), so `first_in_params` is never read.
        # Searching it would spend a third of the dimensions on values that
        # cannot move the score — against an objective the study already
        # struggles to separate on. `two_level_flat` is exactly this config.
        sm_cfg = self.base_config.get("serve_model", {}) if self.is_iid else {}
        self.first_in_is_fitted = bool(
            sm_cfg.get("first_in_match_features")
            or sm_cfg.get("first_in_point_features")
        )
        # Joint whenever the config has two SEARCHABLE blocks and the caller
        # named none. Through the shared predicate, not a local expression —
        # `iid-sweep` kept its own copy of this and lost a condition off it.
        self.joint_two_level = is_joint_two_level(sm_cfg, self._requested_block)
        self.param_namespace = (
            NAMESPACE_JOINT if self.joint_two_level else NAMESPACE_FLAT
        )
        # The single block a non-joint run writes. Meaningless when joint.
        self.serve_block = self._requested_block or "params"

        if self._requested_block is not None:
            if self._requested_block not in _SERVE_BLOCKS:
                raise ValueError(
                    f"serve_block must be one of {sorted(_SERVE_BLOCKS)}; "
                    f"got {self._requested_block!r}"
                )
            if not self.is_iid:
                raise ValueError(
                    f"serve_block={self._requested_block!r} is meaningless for a "
                    "classification config — it has no serve_model."
                )
            if not self.is_two_level:
                raise ValueError(
                    f"serve_block={self._requested_block!r} requires "
                    f"serve_model.type=two_level; got {stype!r}. A single-level "
                    "model is one fit and has only `params`."
                )
            if (
                self._requested_block == "first_in_params"
                and not self.first_in_is_fitted
            ):
                raise ValueError(
                    "serve_block='first_in_params' but the first_in arm has no "
                    "features, so it is intercept-only and never reads that "
                    "block. Give it features or tune `params`."
                )
            logger.warning(
                "%s: --serve-block searches ONE of two coupled blocks. The "
                "objective only exists on the composed p, so this trades the "
                "joint search for a conditional one — the result is the best "
                "%s given whatever the other block is fixed at.",
                self.config_path.stem, self._requested_block,
            )

        # Objective source. CLASSIFICATION and IID: metrics.objective from the
        # config — one source, no --metric, no default, absent = hard error
        # (never a silent fallback to an arbitrary metric). A multi-element list
        # = multi-objective (Pareto). Only the regression PROJECTION path still
        # takes `metrics=` (--metric); it has no metrics.objective field.
        #
        # IID moved off the `--metric`-with-`mae`-default mechanism because the
        # default silently optimized a point-estimate metric on configs whose
        # features had been selected against a distributional one.
        if self.model_type in _PROJECTION_MODEL_TYPES:
            self.metrics = metrics or ["mae"]
        else:
            objective = (self.base_config.get("metrics") or {}).get("objective")
            if not objective:
                example = "iid_crps_total_games" if self.is_iid else "log_loss"
                raise ValueError(
                    f"tuning requires metrics.objective in {self.config_path} "
                    f"(the metric(s) to optimize); none is set. Add e.g.:\n"
                    f"  metrics:\n    objective: {example}"
                )
            # base_config is the RAW yaml dict, not the validated model, so a
            # scalar `objective: log_loss` arrives as a string. Iterating that
            # would build one study direction per CHARACTER.
            self.metrics = [objective] if isinstance(objective, str) else list(objective)

        # Search frame. Probability-scale objectives search on the calibrated
        # out-of-fold metric (raw-frame HPs are not the calibrated optimum); the
        # pure ranking metrics (roc_auc, partial_auc_tail) are Platt-invariant, so
        # raw-frame search is exact for them. The in-search calibration is
        # OUT-OF-FOLD (each fold calibrated by a fitter that never saw it), which
        # is why it does NOT reintroduce the in-sample calibrator-gaming that keeps
        # the config's own `calibration:` block out of tuning. Classification only:
        # IID and projection (regression) tuning have no Platt path.
        is_projection = self.model_type in _PROJECTION_MODEL_TYPES
        self.search_calibrated = (
            (not self.is_iid)
            and (not is_projection)
            and any(m not in CALIBRATION_INVARIANT_METRICS for m in self.metrics)
        )
        self.objective_frame = (
            _OBJECTIVE_FRAME_CAL if self.search_calibrated else _OBJECTIVE_FRAME
        )

        # Tuning ignores the config's `calibration:` block. Calibrated-frame
        # search (when it runs) uses a fixed global Platt fit OUT-OF-FOLD, not the
        # config's calibrator — an in-sample fit of a richer calibrator would let
        # HPs game it. The config's block stays a deployment concern honored by
        # `mvp model`. Warn so the user doesn't expect it to influence tuning.
        if not self.is_iid and self.base_config.get("calibration"):
            logger.warning(
                "config has a `calibration:` block — this is IGNORED during "
                "tuning. Calibration applies only at `mvp model` training "
                "time. Tuning evaluates raw predictor discrimination."
            )

        if search_space is not None:
            self.search_space = dict(search_space)
        elif self.model_type in DEFAULT_SEARCH_SPACES:
            base_space = DEFAULT_SEARCH_SPACES[self.model_type]
            self.search_space = (
                two_level_joint_space(base_space)
                if self.joint_two_level
                else dict(base_space)
            )
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

        # Pin specific params, removing them from the search space. A param is
        # pinnable only if this model type actually tunes it — the search space
        # above is the authority, after every conditional adjustment (dart,
        # early stopping, MTL) has been applied. Rejecting anything else here,
        # before the Optuna storage below exists, is what keeps a foreign param
        # (e.g. tree_method on a logistic study) from being written into the
        # study's pinned_params user attr, where it would survive every later
        # run and get merged into configs by tune-review / the frozen sweep.
        # Non-searched model params belong in the config's model.params, not
        # here — see the `booster: dart` pattern above.
        self.pinned_params: dict[str, Any] = {}
        if param_overrides:
            unknown = sorted(k for k in param_overrides if k not in self.search_space)
            if unknown:
                raise ValueError(
                    f"--param {unknown} not tunable for model type "
                    f"'{self.model_type}'. Pinnable params: "
                    f"{sorted(self.search_space)}"
                )
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
        # One study per (config, block). The two blocks suggest the SAME param
        # names — max_depth, learning_rate, ... — so sharing a study name under
        # `load_if_exists=True` would resume the win-branch trials as if they
        # were first_in ones and silently reinterpret every value.
        self.study_key = tuning_study_key(
            self.config_path.stem,
            None if self.joint_two_level else self._requested_block,
        )

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
                seed=self.seed,
            )

        # Pruning DISABLED (2026-07). The tune metric carries no between-trial
        # signal in the current regime (sep < 1 across every logged axis), so
        # MedianPruner culls ~20% of trials on a noise ranking of the final
        # objective — the culled trials never reach the backtest, and the cull
        # criterion is a betting-irrelevant proxy, so we may be dropping good
        # bettors sight-unseen. There is no signal-bearing target to validate
        # the prunes against, so we run the full pool. To re-enable (e.g. once a
        # metric actually separates trials), swap NopPruner for the MedianPruner
        # below.
        pruner = optuna.pruners.NopPruner()
        # pruner = optuna.pruners.MedianPruner(
        #     n_startup_trials=startup_trials,
        #     n_warmup_steps=2,
        #     n_min_trials=10,
        #     interval_steps=1,
        # )

        self.study = optuna.create_study(
            study_name=self.study_key,
            storage=storage,
            directions=directions,
            load_if_exists=True,
            sampler=sampler,
            pruner=pruner,
        )

        # Forward-aligned objective marker (classification only). The v2 objective
        # (forward-OOS over the inner folds) is numerically incomparable to the
        # legacy within-window inner-CV objective, so a study can't be resumed
        # across frames — TPE would model a discontinuous surface and the pruner a
        # scale mismatch. Stamp fresh studies; refuse to resume across a frame or a
        # changed held-out block. IID/projection tuning doesn't use this machinery.
        if not self.is_iid:
            existing_frame = self.study.user_attrs.get("objective_frame")
            if len(self.study.trials) > 0:
                if existing_frame != self.objective_frame:
                    raise ValueError(
                        f"study '{self.config_path.stem}' has "
                        f"{len(self.study.trials)} trial(s) from "
                        f"objective_frame={existing_frame!r}, not "
                        f"'{self.objective_frame}'. The objective is not comparable "
                        "across frames (raw- vs calibrated-frame search, or the "
                        "legacy within-window objective) — use a fresh study "
                        f"(rename the config or delete {self.db_path})."
                    )
                prior_outer = self.study.user_attrs.get("outer_folds")
                if prior_outer != self.outer_folds:
                    raise ValueError(
                        f"study '{self.config_path.stem}' was built with "
                        f"outer_folds={prior_outer}, but this run requests "
                        f"{self.outer_folds}. Changing the held-out block mid-study "
                        "makes trials incomparable — use a fresh study."
                    )
            else:
                self.study.set_user_attr("objective_frame", self.objective_frame)
                self.study.set_user_attr("outer_folds", self.outer_folds)

        # Param namespace, same stamp-and-refuse shape as objective_frame above
        # and for the same reason: a study's trials are encoded ONE way, and
        # mixing encodings makes them mutually unreadable rather than merely
        # incomparable. Jointness is derived from the config's first_in feature
        # lists, so editing those under an existing study would silently flip the
        # namespace and start writing `win_max_depth` into a study whose earlier
        # trials say `max_depth` — leaving a study no reader can interpret as a
        # whole. Studies written before this stamp existed carry no attr; they
        # are read back through `study_param_namespace`, which infers from the
        # trials, and are stamped here once that inference agrees.
        prior_namespace = self.study.user_attrs.get(PARAM_NAMESPACE_ATTR)
        if len(self.study.trials) > 0:
            observed = study_param_namespace(self.study)
            if observed != self.param_namespace:
                raise ValueError(
                    f"study '{self.study_key}' holds {len(self.study.trials)} "
                    f"trial(s) in the '{observed}' param namespace, but this run "
                    f"would write '{self.param_namespace}'. A two-level config "
                    "searches both blocks jointly (prefixed param names) only "
                    "while its first_in arm has features; changing "
                    "`first_in_match_features` / `first_in_point_features` or "
                    "passing --serve-block flips that. Trials in two namespaces "
                    f"cannot be read together — use a fresh study (rename the "
                    f"config or delete {self.db_path})."
                )
            if prior_namespace is None:
                self.study.set_user_attr(PARAM_NAMESPACE_ATTR, observed)
        else:
            self.study.set_user_attr(PARAM_NAMESPACE_ATTR, self.param_namespace)

        # Objective metric name(s). A trial's `values[0]` is a bare number —
        # nothing in the study said WHICH metric it was, nor in which frame, so
        # `tune-review` could only infer it by matching the value against the
        # stored metric attrs. Written on every run (not just fresh studies) so
        # studies tuned before this stamp existed pick it up on their next
        # resume. Metadata only: nothing in the search reads it back.
        self.study.set_user_attr("objective_metrics", list(self.metrics))

        if self.pinned_params:
            self.study.set_user_attr("pinned_params", self.pinned_params)

        # Enqueue baseline trial from config params
        self._enqueue_baseline()

    def _preflight_fold_check(self) -> None:
        """Fail fast (at construction, before any Optuna storage side effects) if a
        calendar date-window config's span can't feed a trustworthy forward-aligned
        search. Covers date_sliding and date_expanding — the validation types real
        classification configs use. Index-based types (walk_forward etc.) derive
        fold count from the data and are not preflighted; the runner's
        holdout_folds < n_folds guard only prevents a crash there, it does not
        enforce the inner-fold minimum."""
        if self.is_iid:
            return
        val = self.base_config.get("validation") or {}
        val_type = val.get("type")
        if val_type == "date_sliding":
            train_months = val.get("train_months")
        elif val_type == "date_expanding":
            train_months = val.get("initial_train_months")
        else:
            return
        test_months = val.get("test_months")
        dr = (self.base_config.get("data") or {}).get("date_range") or {}
        start, end = dr.get("start"), dr.get("end")
        if not (train_months and test_months and start and end):
            return
        try:
            s = start if isinstance(start, date) else datetime.strptime(str(start), "%Y-%m-%d").date()
            e = end if isinstance(end, date) else datetime.strptime(str(end), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return
        # Month-granularity estimate of the splitter's fold count: the first test
        # window opens `train_months` after the start, then contiguous `test_months`
        # windows follow. The +1 matches the splitter's ceil-to-next-month upper
        # bound (real configs use month-boundary date ranges).
        span_months = (e.year - s.year) * 12 + (e.month - s.month)
        n_outer = (span_months + 1 - int(train_months)) // int(test_months)
        n_inner = n_outer - self.outer_folds
        if n_inner < self._HARD_MIN_TUNING_FOLDS:
            raise ValueError(
                f"{self.config_path.stem}: data.date_range spans ~{n_outer} forward "
                f"fold(s); with outer_folds={self.outer_folds} that leaves ~{n_inner} "
                f"for the search — a forward-aligned tune needs at least "
                f"{self._HARD_MIN_TUNING_FOLDS} search folds (below that the "
                "calibrated-frame objective can't be computed and the study silently "
                "searches raw). Lower --outer-folds or widen data.date_range."
            )
        if n_inner < self._MIN_TUNING_FOLDS:
            logger.warning(
                "%s: data.date_range spans ~%d forward fold(s); with outer_folds=%d "
                "the search rests on ~%d forward fold(s), below the recommended %d for "
                "a trustworthy forward-aligned tune. Proceeding — treat the resulting "
                "HPs as thin (few forward folds) and sanity-check them.",
                self.config_path.stem,
                n_outer,
                self.outer_folds,
                n_inner,
                self._MIN_TUNING_FOLDS,
            )

    def _get_base_params(self) -> dict[str, Any]:
        """The config's incumbent params, in the study's own key namespace.

        Feeds the baseline trial and the per-trial merge, so it has to match
        whatever `search_space` suggests: prefixed for a joint two-level study,
        bare otherwise.
        """
        if not self.is_iid:
            return self.base_config.get("model", {}).get("params") or {}
        sm = self.base_config.get("serve_model", {})
        # `or {}` throughout, not `.get(k, {})`: an explicit `params:` with no
        # body parses as None, and callers build `dict(...)` from this.
        win = sm.get("params") or {}
        # An empty `first_in_params` means the arm INHERITS `params`
        # (serve_model.py:140), so that is its true starting point — not an
        # empty baseline. Enqueueing `{}` would make trial 0 a random draw and
        # throw away the incumbent the config actually runs.
        first_in = sm.get("first_in_params") or win
        if self.joint_two_level:
            return {
                **{f"{WIN_PREFIX}{k}": v for k, v in win.items()},
                **{f"{FIRST_IN_PREFIX}{k}": v for k, v in first_in.items()},
            }
        return win if self.serve_block == "params" else first_in

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
        if not baseline:
            return
        # A partial baseline is still worth enqueueing: Optuna samples the keys
        # the config doesn't pin, so trial 0 is "your config, with the rest
        # explored" rather than a fully random draw. Requiring a value for every
        # search-space key meant configs that set only the handful of params they
        # care about — the normal case for a promoted FS config — silently never
        # evaluated their own hyperparameters.
        if len(baseline) < len(self.search_space):
            missing = sorted(set(self.search_space) - set(baseline))
            logger.info(
                "Baseline trial: config sets %d/%d search-space params; the rest "
                "are sampled (%s)",
                len(baseline), len(self.search_space), ", ".join(missing),
            )
        self.study.enqueue_trial(baseline)

    def _objective_metric_value(self, result: dict, metric: str) -> float:
        """The objective value for `metric` in this study's search frame.

        Ranking metrics (Platt-invariant) and raw-frame studies use the raw
        pooled OOF metric. In a calibrated-frame study, probability-scale metrics
        use the calibrated-frame value (`metrics_calibrated`), falling back to raw
        if the calibrated objective couldn't be computed (e.g. single-class fold,
        so `_calibrated_objective_metrics` returned None)."""
        if metric in CALIBRATION_INVARIANT_METRICS or not self.search_calibrated:
            return result["metrics"][metric]
        mc = result.get("metrics_calibrated")
        if mc is None:
            return result["metrics"][metric]
        return mc[metric]

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

        # One-time warning if a calibrated-frame study silently fell back to the
        # raw objective. The failure is deterministic on the fold split (<2 tuning
        # folds, or a single-class fold complement), so it fires for every trial,
        # not inconsistently — but the study would still be stamped calibrated
        # while effectively searching raw. Surface it rather than mislabel silently.
        if (
            self.search_calibrated
            and not self._cal_fallback_warned
            and result.get("metrics_calibrated") is None
        ):
            logger.warning(
                "%s: calibrated-frame search requested but the calibrated objective "
                "could not be computed (needs >=2 tuning folds with two-class "
                "complements); falling back to the RAW objective for all trials. "
                "The study is stamped calibrated-frame but is effectively raw — "
                "widen the tuning span or check fold label balance.",
                self.config_path.stem,
            )
            self._cal_fallback_warned = True

        # Mark trials with their search frame so `tune-review` can (a) distinguish
        # new-pipeline studies from legacy (in-sample Platt) ones and (b) know
        # whether the objective was raw or calibrated. IID/projection are always
        # "raw" (no Platt path).
        trial.set_user_attr(
            "_tuning_mode", "calibrated" if self.search_calibrated else "raw"
        )

        # Store all metrics as user attrs for review
        for metric_name, metric_value in result["metrics"].items():
            trial.set_user_attr(metric_name, metric_value)

        # Calibrated-frame in-fold objective metrics (calibrated-frame studies) —
        # the actual optimization target for probability-scale objectives.
        if result.get("metrics_calibrated"):
            for metric_name, metric_value in result["metrics_calibrated"].items():
                trial.set_user_attr(f"cal_{metric_name}", metric_value)

        # Holdout metrics (the search-blind outer block, `outer_folds` trailing
        # forward folds) — used by tune-review to re-rank trials by the honest
        # metric, not the tuning-set (inner-fold) metric.
        if result.get("holdout_metrics"):
            for metric_name, metric_value in result["holdout_metrics"].items():
                trial.set_user_attr(f"holdout_{metric_name}", metric_value)

        # Deployment-frame (global-Platt) outer-block metrics — the honest,
        # comparison-grade numbers for probability-scale metrics (raw AUC is
        # calibration-invariant, so the raw holdout already suffices there).
        if result.get("holdout_metrics_calibrated"):
            for metric_name, metric_value in result["holdout_metrics_calibrated"].items():
                trial.set_user_attr(f"holdout_cal_{metric_name}", metric_value)

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
        if result.get("holdout_fold_metrics_calibrated"):
            trial.set_user_attr(
                "holdout_fold_metrics_calibrated",
                result["holdout_fold_metrics_calibrated"],
            )

        trial.set_user_attr("duration_s", result["duration_s"])

        if len(self.metrics) == 1:
            return self._objective_metric_value(result, self.metrics[0])
        return tuple(
            self._objective_metric_value(result, m) for m in self.metrics
        )

    def _build_trial_config(self, params: dict[str, Any]) -> Path:
        """One trial's config, written to a temp file for the runner to load.

        Extracted from `_run_one` so which block a trial writes is checkable
        without standing up a fit — that choice is the whole of the two-level
        tuning fix, and it was previously reachable only by running a model.
        """
        config = dict(self.base_config)
        if self.is_iid:
            config["serve_model"] = dict(config["serve_model"])
            # Start from each block's effective values, so a trial that leaves a
            # key unsampled keeps what the config runs rather than dropping it.
            merged = dict(self._get_base_params())
            merged.update(params)
            if self.joint_two_level:
                win, first_in = split_joint_params(merged)
                if self._per_trial_n_jobs is not None:
                    win["n_jobs"] = self._per_trial_n_jobs
                    first_in["n_jobs"] = self._per_trial_n_jobs
                # BOTH blocks, every trial. A joint trial is one complete model;
                # writing only one would leave the other inheriting this trial's
                # win-branch values and score something the trial did not name.
                config["serve_model"]["params"] = win
                config["serve_model"]["first_in_params"] = first_in
            else:
                if self._per_trial_n_jobs is not None:
                    merged["n_jobs"] = self._per_trial_n_jobs
                config["serve_model"][self.serve_block] = merged
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
            return Path(f.name)

    def _run_one(
        self, params: dict[str, Any], trial: optuna.Trial | None = None,
    ) -> dict[str, Any]:
        """Run a single param combination through the appropriate runner.

        When `trial` is provided, the underlying runner reports the per-fold
        tuning objective (metrics.objective) and may raise optuna.TrialPruned
        mid-run.
        """
        temp_path = self._build_trial_config(params)

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
                    # Trials run against a temp config; persisting would leave a
                    # junk fingerprint dir per trial holding nothing the study
                    # doesn't already carry.
                    persist=False,
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
                    holdout_folds=self.outer_folds,
                    inner_cv_folds=0,
                    calibrate=False,
                    report_calibrated_holdout=True,
                    report_calibrated_objective=self.search_calibrated,
                )
            # IID / projection runners don't currently support pruning;
            # only ExperimentRunner threads `trial` through. Pass it where
            # accepted, ignore where not.
            if self.is_iid or self.model_type in _PROJECTION_MODEL_TYPES:
                result = runner.run()
            else:
                result = runner.run(trial=trial)
            metrics = dict(result["metrics"])
            metrics_calibrated = (
                dict(result["metrics_calibrated"])
                if result.get("metrics_calibrated") is not None
                else None
            )
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
            holdout_metrics_calibrated = (
                dict(result["holdout_metrics_calibrated"])
                if result.get("holdout_metrics_calibrated") is not None
                else None
            )
            holdout_fold_metrics_calibrated = (
                [dict(f) for f in result["holdout_fold_metrics_calibrated"]]
                if result.get("holdout_fold_metrics_calibrated") is not None
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
                "metrics_calibrated": metrics_calibrated,
                "holdout_metrics": holdout_metrics,
                "holdout_metrics_calibrated": holdout_metrics_calibrated,
                "inner_cv_folds": inner_cv_folds_used,
                "inner_fold_count_per_outer": inner_fold_count_per_outer,
                "fold_metrics": fold_metrics,
                "holdout_fold_metrics": holdout_fold_metrics,
                "holdout_fold_metrics_calibrated": holdout_fold_metrics_calibrated,
                "duration_s": round(duration, 1),
            }
        finally:
            temp_path.unlink(missing_ok=True)

    def run(
        self, n_trials: int, verbose: bool = True, parallel_trials: int = 1,
    ) -> optuna.Study:
        """Run Bayesian optimization for n_trials.

        verbose=True logs the full per-trial line (params | metrics | duration).
        verbose=False shows a single tqdm bar instead — the study DB keeps every
        trial either way, so nothing is lost, just scrollback. The bar needs a
        TTY; redirected output falls back to the per-trial lines rather than
        going silent.

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

        # The bar is the default display; it needs a TTY to be worth anything,
        # so a redirected run keeps the per-trial lines instead of emitting a
        # few thousand carriage returns into a log file.
        use_bar = not verbose and sys.stderr.isatty()

        callbacks = []
        if use_bar:
            self._bar = tqdm(
                total=n_trials,
                desc=f"{self.config_path.stem} [{self._objective_label()}]",
                ncols=120,
            )
            self._bar_best = self._best_objective_so_far()
            self._bar_best_is_session = False
            self._bar_pruned = 0
            self._set_bar_postfix()
            callbacks.append(self._progress_trial_callback)
        else:
            callbacks.append(self._log_trial_callback)

        # Quiet the per-fold runner/engine logs for the whole tune. Done ONCE
        # here (not per-trial in _run_one) so concurrent trials under
        # parallel_trials>1 don't race on these shared logger levels. Under the
        # bar, early_stopping is quieted too — its per-fold best_iteration lines
        # are useful alongside the verbose trial lines but shred a progress bar.
        _quiet_names = [
            "mvp.model.runner", "mvp.model.engine",
            "mvp.projection.runner", "mvp.projection.iid.runner",
        ]
        if use_bar:
            _quiet_names.append("mvp.model.early_stopping")
        _quiet = [logging.getLogger(name) for name in _quiet_names]
        _prev_levels = [lg.level for lg in _quiet]
        for lg in _quiet:
            lg.setLevel(logging.WARNING)
        # Route log records through tqdm.write while the bar is live: a plain
        # handler writes straight to stderr mid-render, which terminates the
        # bar's line and strands it (the next refresh then redraws on a fresh
        # line). Redirected, each record clears the bar, prints above it, and
        # the bar redraws underneath.
        redirect = logging_redirect_tqdm() if use_bar else nullcontext()
        try:
            with redirect:
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
                    self.study.optimize(
                        self._objective, n_trials=1, callbacks=callbacks
                    )
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
            if self._bar is not None:
                self._bar.close()
                self._bar = None

        logger.info(
            "Tuning complete: %d total trials in %s",
            len(self.study.trials), self.db_path,
        )
        return self.study

    def _objective_label(self) -> str:
        """Name of the value the bar tracks — the study's first objective in its
        search frame. A calibrated-frame study optimizes the calibrated value of
        a probability-scale metric, so label it cal_* to match the user_attr the
        trials carry (and to not read as the raw metric the verbose line shows)."""
        metric = self.metrics[0]
        if self.search_calibrated and metric not in CALIBRATION_INVARIANT_METRICS:
            return f"cal_{metric}"
        return metric

    def _maximizing(self) -> bool:
        """Direction of the first (bar-displayed) objective."""
        return self.study.directions[0] == optuna.study.StudyDirection.MAXIMIZE

    def _best_objective_so_far(self) -> float | None:
        """Best first-objective value among trials already in the study, so a
        resumed run's bar opens with the study's real best rather than
        restarting from the first trial of this session."""
        values = [
            t.values[0] for t in self.study.trials
            if t.state == optuna.trial.TrialState.COMPLETE and t.values
        ]
        if not values:
            return None
        return max(values) if self._maximizing() else min(values)

    def _set_bar_postfix(self) -> None:
        """Study-wide best (starred when THIS session set it) and the pruned
        count. Caller holds the bar lock; refresh is left to the update() that
        follows."""
        fields: dict[str, Any] = {}
        if self._bar_best is not None:
            star = "*" if self._bar_best_is_session else ""
            fields["best"] = f"{self._bar_best:.4f}{star}"
        if self._bar_pruned:
            fields["pruned"] = self._bar_pruned
        if fields:
            self._bar.set_postfix(refresh=False, **fields)

    def _progress_trial_callback(
        self, study: optuna.Study, trial: optuna.trial.FrozenTrial
    ) -> None:
        """Advance the bar in place of the per-trial log line. Params and every
        metric still land in the study DB for `tune-review`."""
        with self._bar_lock:
            if self._bar is None:
                return
            if trial.state == optuna.trial.TrialState.PRUNED:
                self._bar_pruned += 1
            elif trial.values:
                value = trial.values[0]
                if self._bar_best is None or (
                    value > self._bar_best if self._maximizing()
                    else value < self._bar_best
                ):
                    self._bar_best = value
                    self._bar_best_is_session = True
            self._set_bar_postfix()
            self._bar.update(1)

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
