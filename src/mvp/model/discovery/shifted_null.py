"""Shifted-candidate null for family-level forward selection.

Plan: mvp-docs/plans/2026-08-25-fs-protocol-redesign.md, design item 2 /
build item 3. The tested statistic is a family's gain over the current
accepted set, so the null must condition on that set: the outcome and the
accepted features stay real, and the CANDIDATE family is nulled by an
independent random circular time-shift within each player's own sequence of
the family's underlying per-side values. That preserves each player's
autocorrelation exactly — a match-level scramble is whiter than a real
rolling stat and anti-conservative for precisely the rolling-window families
this targets.

Mechanics
---------
- Shift domain is the fold's TRAINING rows only; test rows keep real values
  (the trained-on-scrambled model's mapping is what carries the null).
- One offset per (player, replicate), applied to every parent column of the
  family on both sides: a player's stat sequence is the same series whether
  read from their own rows (``player_`` columns) or their opponents' rows
  (``opp_`` columns), and both orderings sort identically by
  (date, match_uid), so a shared offset keeps the two appearances coherent.
- Combiner members (``_diff``/``_sum``/``_matchup``) are rebuilt row-locally
  from the shifted parents — registry combiners are literal
  ``player_parent -/+ opp_parent`` arithmetic (registry.register_diff/
  register_sum/register_matchup). A matchup's cross-stat parent (the dep
  outside the family) stays REAL: the null destroys only the candidate
  stat's alignment.
- Baseline (accepted-only), observed (accepted + real family) and null
  (accepted + shifted family) fold metrics all go through the SAME fold-fit
  path here, so gains and null gains are exactly comparable; a test asserts
  this path reproduces the fast-selection scorer's metrics.

Known approximations (each noted where it applies): players with a single
train-window match keep real values (nothing to shift); per-fold medians for
rebuilt combiner members reuse the real columns' medians; a player whose two
side-orderings differ in length (an orientation row dropped by filters)
shifts each side by ``offset % m`` independently.

The composite statistic is (mean fold gain, positive in >= ``min_agree``
folds) and the null replicates are pushed through the same rule, so
expanding-fold dependence is priced into the calibration. The acceptance
bars (the max-null — "beats the best of the fakes" — and the negative-control
floor read off gains the round already computed) are small pure functions at
the bottom of the module.

Why the max-null and not per-family p-values under FDR: with K replicates a
family's own-null p-value is at best 1/(K+1), and Benjamini-Hochberg over n
families needs p <= q*r/n — at K=20, n=532, q=0.10 nothing can be accepted
until ~254 families all sit at the floor. Taking, per replicate, the best
null composite across every tested family puts the multiplicity in the
statistic instead of in the p-value resolution: K=20 calibrates the round's
test however many families were scored. Own-null p-values are still
recorded per family as a diagnostic.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np

from mvp.model.discovery.families import family_of
from mvp.model.discovery.fast_selection import (
    FastForwardSelector,
    _make_metric_fn,
    _masked_log_loss,
    pair_index,
    symmetrize_indexed,
)
from mvp.model.engine import build_column_name, parse_feature_spec
from mvp.model.models import get_model
from mvp.model.registry import get_registry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rebuild plans: member spec -> parent engine columns + row-local arithmetic
# ---------------------------------------------------------------------------


@dataclass
class RebuildMember:
    """How one member column is produced from parent columns.

    kind "side": the member IS a per-side parent (use its shifted values).
    kind "combine": member = left + sign * right, row-locally.
    """

    col: str
    kind: str  # "side" | "combine"
    left: str | None = None
    right: str | None = None
    sign: int = -1  # diff/matchup: -1; sum: +1


@dataclass
class FamilyRebuildPlan:
    family: str
    members: list[RebuildMember]
    # Engine columns of the family's own stat (both sides) that get shifted.
    shift_cols: list[str]
    # Parent engine columns required but absent from the matrix. Non-empty
    # means the family cannot be tested by this bar — reported, not guessed.
    missing: list[str]
    # Matchup members whose registry entry declares no parents (custom
    # expressions): shifted as their own per-side column instead of rebuilt.
    # The opponent's half of the stat moves with the player's sequence — a
    # coarser null than the rebuild, still detached from the match.
    approximated: list[str] = field(default_factory=list)


def _windowed(name: str, params: dict) -> str:
    return build_column_name(name, params)


def _spec(name: str, params: dict) -> str:
    return f"{name}(days={params['days']})" if "days" in params else name


def rebuild_parent_specs(member_specs: list[str]) -> set[str]:
    """Feature specs of the per-side parents the rebuild needs for these
    members (``player_<stem>(days=N)`` form). Rating families carry only
    their diff/matchup forms in a candidate pool, so their parents are not
    in the FS matrix unless requested up front — discover.py adds these as
    matrix-only columns (never candidates)."""
    registry = get_registry()
    out: set[str] = set()
    for spec in member_specs:
        prefix, base, _full_name, params = parse_feature_spec(spec)
        if base.endswith("_diff") or base.endswith("_sum"):
            stem = base[: base.rfind("_")]
            out.update({_spec(f"player_{stem}", params), _spec(f"opp_{stem}", params)})
        elif base.endswith("_matchup"):
            try:
                deps = list(registry.get(base).depends_on or [])
            except KeyError:
                deps = []
            if len(deps) != 2:
                continue  # shifted as its own column (see resolve_rebuild)
            dep1, dep2 = deps
            if prefix == "opp":
                out.update({_spec(f"opp_{dep1}", params), _spec(f"player_{dep2}", params)})
            else:
                out.update({_spec(f"player_{dep1}", params), _spec(f"opp_{dep2}", params)})
    return out


def resolve_rebuild(
    family: str, member_specs: list[str], col_to_idx: dict[str, int]
) -> FamilyRebuildPlan:
    """Build the shift/rebuild plan for one family's member columns."""
    registry = get_registry()
    members: list[RebuildMember] = []
    shift_cols: set[str] = set()
    missing: set[str] = set()
    approximated: set[str] = set()

    def _need(col: str) -> str:
        if col not in col_to_idx:
            missing.add(col)
        return col

    def _own(col: str, parent_full_name: str, params: dict) -> str:
        """Register a parent column; shift it iff its stat is this family's."""
        col = _need(col)
        window = f"(days={params['days']})" if "days" in params else ""
        if family_of(f"{parent_full_name}{window}") == family:
            shift_cols.add(col)
        return col

    for spec in member_specs:
        prefix, base, full_name, params = parse_feature_spec(spec)
        member_col = _windowed(full_name, params)
        _need(member_col)

        if base.endswith("_diff") or base.endswith("_sum"):
            stem = base[: base.rfind("_")]
            left = _own(
                _windowed(f"player_{stem}", params), f"player_{stem}", params
            )
            right = _own(_windowed(f"opp_{stem}", params), f"opp_{stem}", params)
            members.append(RebuildMember(
                col=member_col, kind="combine", left=left, right=right,
                sign=1 if base.endswith("_sum") else -1,
            ))
            continue

        if base.endswith("_matchup"):
            # register_matchup convention: expr = player_{dep1} - opp_{dep2};
            # the mirrored (opp_-prefixed) column flips both sides.
            try:
                fdef = registry.get(base)
            except KeyError:
                fdef = None
            deps = list(getattr(fdef, "depends_on", []) or [])
            if fdef is None or len(deps) != 2:
                # No declared parents (custom matchup expression): shift the
                # member as its own per-side column. Approximation, logged
                # on the plan, not a reason to leave the family untestable.
                members.append(RebuildMember(col=member_col, kind="side"))
                if member_col in col_to_idx:
                    shift_cols.add(member_col)
                    approximated.add(member_col)
                continue
            dep1, dep2 = deps
            if prefix == "opp":
                left_name, right_name = f"opp_{dep1}", f"player_{dep2}"
            else:
                left_name, right_name = f"player_{dep1}", f"opp_{dep2}"
            left = _own(_windowed(left_name, params), left_name, params)
            right = _own(_windowed(right_name, params), right_name, params)
            members.append(RebuildMember(
                col=member_col, kind="combine", left=left, right=right, sign=-1,
            ))
            continue

        # Plain per-side member: it is its own (shifted) parent.
        members.append(RebuildMember(col=member_col, kind="side"))
        if prefix in ("player", "opp"):
            shift_cols.add(member_col)
        else:
            # Unprefixed non-combiner (context flags etc.): no player sequence
            # to shift — untestable by this null.
            missing.add(member_col)

    return FamilyRebuildPlan(
        family=family,
        members=members,
        shift_cols=sorted(shift_cols),
        missing=sorted(missing),
        approximated=sorted(approximated),
    )


# ---------------------------------------------------------------------------
# Within-player circular shifts over a fold's train rows
# ---------------------------------------------------------------------------


def _side_gather_map(
    side_ids: np.ndarray,
    dates: np.ndarray,
    uids: np.ndarray,
    train_idx: np.ndarray,
    offsets: dict,
) -> np.ndarray:
    """Full-length source-row map: identity everywhere except the fold's train
    rows, where each identity's rows (grouped by this side's id, ordered by
    (date, match_uid)) are circularly shifted by that identity's offset."""
    src = np.arange(side_ids.shape[0])
    t_ids = side_ids[train_idx]
    order = np.lexsort((uids[train_idx], dates[train_idx], t_ids))
    sorted_rows = train_idx[order]
    sorted_ids = t_ids[order]
    boundaries = np.flatnonzero(sorted_ids[1:] != sorted_ids[:-1]) + 1
    starts = np.concatenate(([0], boundaries))
    ends = np.concatenate((boundaries, [sorted_ids.shape[0]]))
    for a, b in zip(starts, ends):
        m = b - a
        if m < 2:
            continue  # single-match sequence: nothing to shift, keep real
        s = offsets.get(sorted_ids[a])
        if not s:
            continue
        s = s % m or 1
        rows = sorted_rows[a:b]
        src[rows] = rows[(np.arange(m) - s) % m]
    return src


def _sample_offsets(
    player_ids: np.ndarray, train_idx: np.ndarray, rng: np.random.Generator
) -> dict:
    """One circular offset per player appearing in the fold's train rows,
    drawn from {1..m-1} of their own-side match count."""
    ids, counts = np.unique(player_ids[train_idx], return_counts=True)
    return {
        i: int(rng.integers(1, m)) for i, m in zip(ids, counts) if m >= 2
    }


# ---------------------------------------------------------------------------
# Fold fit (mirrors the fast-selection scorer's plain-XGB fold body)
# ---------------------------------------------------------------------------


class _FoldScorer:
    """Scores (accepted + member) column sets per fold, with optional
    substituted train-row values for the member columns.

    Replicates the fast_selection scorer's plain path: np.ix_ gather,
    constant/median impute with per-fold train medians, offset base_margin on
    both fit and predict, orientation projection on the test fold, single
    metric. MTL / early-stopping / scaled models are out of scope (the stage
    configs this serves are plain XGBoost + offset) and raise.
    """

    def __init__(
        self, fast: FastForwardSelector, metric: str, n_jobs: int | None = None
    ):
        if fast.config.mtl is not None:
            raise NotImplementedError("shifted null: MTL configs unsupported")
        es = fast.config.early_stopping
        if es is not None and es.enabled:
            raise NotImplementedError(
                "shifted null: early_stopping configs unsupported"
            )
        if fast.config.model.type in ("logistic", "neural_net"):
            raise NotImplementedError(
                "shifted null: scaled model types unsupported"
            )
        if fast.row_player_ids is None or fast.row_opp_ids is None:
            raise ValueError(
                "shifted null needs row_player_ids/row_opp_ids — re-run "
                "precompute() with player_id/opp_id on the frame"
            )
        if fast.row_uids is None:
            raise ValueError(
                "shifted null needs match_uid for stable within-player "
                "ordering and orientation projection"
            )
        self.fast = fast
        params = dict(fast.config.model.params or {})
        self.metric_fn = _make_metric_fn(
            metric, lambda_over=params.get("lambda_over"),
        )
        # Same fixed population as the candidate scorer: when the selector
        # carries a per-round incumbent mask, null replicates are scored on
        # it too (read at call time — the mask changes each round).
        self._masked = metric == "restricted_logloss"
        # Per-fit thread share (OpenMP via XGB n_jobs), injected the same way
        # the fast-selection scorer does it for the candidate loop.
        if n_jobs is not None:
            params["n_jobs"] = int(n_jobs)
        self._model_params = params
        # Orientation-projection pair maps per fold, built up front so
        # score_fold has no shared mutable state and can run on threads.
        self._pairs: dict[int, tuple[np.ndarray, np.ndarray]] = {}
        if fast.config.target == "won":
            for f, (_, test_idx) in enumerate(fast.folds):
                if fast.eval_mask is not None:
                    test_idx = test_idx[fast.eval_mask[test_idx]]
                if test_idx.size:
                    i_p, j_p, _ = pair_index(fast.row_uids[test_idx])
                    self._pairs[f] = (i_p, j_p)

    def score_fold(
        self,
        fold_idx: int,
        col_indices: np.ndarray,
        train_overrides: dict[int, np.ndarray] | None = None,
    ) -> float | None:
        """Metric for one fold. ``train_overrides`` maps a global column index
        to a full-length value array substituted on TRAIN rows only (test rows
        always come from the real matrix). None = fold skipped (empty eval)."""
        fast = self.fast
        train_idx, test_idx = fast.folds[fold_idx]
        if fast.eval_mask is not None:
            test_idx = test_idx[fast.eval_mask[test_idx]]
            if test_idx.size == 0:
                return None

        X_train = fast.X_wide[np.ix_(train_idx, col_indices)]
        X_test = fast.X_wide[np.ix_(test_idx, col_indices)]
        if train_overrides:
            for pos, ci in enumerate(col_indices):
                override = train_overrides.get(int(ci))
                if override is not None:
                    X_train[:, pos] = override[train_idx]
        y_train = fast.y[train_idx]
        y_test = fast.y[test_idx]

        # Impute per the FS-time fill contract. Medians are the REAL fold
        # medians: a within-train permutation leaves per-side train multisets
        # unchanged, and rebuilt combiners reuse their real column's median
        # (approximation noted in the module docstring).
        for pos, ci in enumerate(col_indices):
            strat = fast.fill_strategies[ci]
            if strat == "constant":
                val = fast.fill_constants[ci]
            elif strat == "median":
                val = fast.fold_medians[fold_idx][ci]
            else:  # passthrough: NaN is the contract (XGB consumes natively)
                continue
            col_train = X_train[:, pos]
            col_test = X_test[:, pos]
            col_train[np.isnan(col_train)] = val
            col_test[np.isnan(col_test)] = val

        model = get_model(fast.config.model.type, self._model_params)
        fit_kwargs: dict = {}
        predict_kwargs: dict = {}
        if fast.sample_weights is not None:
            fit_kwargs["sample_weight"] = fast.sample_weights[train_idx]
        if fast.fold_margins is not None:
            margins = fast.fold_margins[fold_idx]
            fit_kwargs["base_margin"] = margins[train_idx]
            predict_kwargs["base_margin"] = margins[test_idx]
        model.fit(X_train, y_train, **fit_kwargs)
        y_prob = model.predict_proba(X_test, **predict_kwargs)

        pair = self._pairs.get(fold_idx)
        if pair is not None:
            y_prob = symmetrize_indexed(y_prob, *pair)
        if self._masked and fast.score_masks is not None:
            keep = fast.score_masks[fold_idx][test_idx]
            if not keep.any():
                return None
            return _masked_log_loss(y_test[keep], y_prob[keep])
        return float(self.metric_fn(y_test, y_prob))


# ---------------------------------------------------------------------------
# Driver: per-family observed vs null composite gains
# ---------------------------------------------------------------------------


@dataclass
class FamilyNullVerdict:
    family: str
    p_value: float | None
    observed_composite: float = float("-inf")
    observed_fold_gains: list[float] = field(default_factory=list)
    null_composites: list[float] = field(default_factory=list)
    reason: str | None = None  # set when untestable (p_value None)


def null_verdicts_path(checkpoint_path: Path | None, round_num: int) -> Path | None:
    """Per-round verdict log beside the FS checkpoint
    (``null_verdicts_<stem>_r<round>.jsonl``). The candidate-score checkpoint
    covers scoring; this covers the null runner, which is the longer part
    of a family-mode round and otherwise restarts from family 1 on resume."""
    if checkpoint_path is None:
        return None
    name = checkpoint_path.name
    prefix = "discovery_checkpoint_"
    stem = name[len(prefix):] if name.startswith(prefix) else name
    stem = stem.rsplit(".", 1)[0]
    return checkpoint_path.with_name(f"null_verdicts_{stem}_r{round_num}.jsonl")


def _verdict_key(accepted_specs: list[str], k: int, seed: int, min_agree: int) -> dict:
    """What a stored verdict is conditional on. A line whose key differs is
    from another accepted set / null draw and must not be reused."""
    return {
        "accepted": sorted(accepted_specs), "k": k, "seed": seed,
        "min_agree": min_agree,
    }


def load_verdicts(path: Path | None, key: dict) -> dict[str, FamilyNullVerdict]:
    """Verdicts stored under exactly ``key``; anything else is ignored."""
    if path is None or not path.exists():
        return {}
    out: dict[str, FamilyNullVerdict] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("key") != key:
                continue
            out[rec["verdict"]["family"]] = FamilyNullVerdict(**rec["verdict"])
    return out


def _append_verdict(fh, verdict: FamilyNullVerdict, key: dict) -> None:
    fh.write(json.dumps({"key": key, "verdict": asdict(verdict)}) + "\n")
    fh.flush()


def composite_gain(fold_gains: list[float], min_agree: int) -> float:
    """Mean fold gain, gated on positive gain in >= min_agree folds; a
    failing gate collapses to -inf so the same rule prices both the observed
    statistic and every null replicate."""
    if not fold_gains:
        return float("-inf")
    agree = sum(1 for g in fold_gains if g > 0)
    if agree < min_agree:
        return float("-inf")
    return float(np.mean(fold_gains))


def run_family_nulls(
    fast: FastForwardSelector,
    metric: str,
    accepted_specs: list[str],
    families: dict[str, list[str]],
    k: int = 20,
    seed: int = 0,
    min_agree: int | None = None,
    direction: str = "minimize",
    workers: int = 1,
    n_jobs: int | None = None,
    checkpoint_path: Path | None = None,
    rebuild_ids: dict[str, str] | None = None,
) -> list[FamilyNullVerdict]:
    """Observed and null composite gains, and a p-value, per family.

    ``rebuild_ids`` maps a candidate id to the family id its rebuild plan
    should use when the two differ — the within-family pick tests single
    members as one-column candidates, and their combiner parents are only
    shifted if the plan knows which family they belong to.

    ``accepted_specs`` is the expanded accepted set (base seeds + accepted
    families' member columns). ``families`` maps family id -> member specs to
    test this round. K replicates per family; every quantity (baseline,
    observed, nulls) runs through the same fold-fit path.

    ``workers`` families are processed concurrently (threads; XGB releases
    the GIL), each fit on ``n_jobs`` threads — the candidate loop's budget,
    which family mode leaves idle. Offsets are drawn up front per (fold,
    replicate), so verdicts do not depend on scheduling order — which is
    also what makes ``checkpoint_path`` sound: each verdict is appended as
    it completes, keyed on (accepted set, k, seed, min_agree), and a rerun
    with the same key skips the families already on disk and computes the
    rest identically.
    """
    from mvp.model.engine import get_feature_columns

    scorer = _FoldScorer(fast, metric, n_jobs=n_jobs)
    n_folds = len(fast.folds)
    if min_agree is None:
        min_agree = max(1, n_folds - 1)  # 3-of-4 at the standard schedule
    sgn = 1.0 if direction == "minimize" else -1.0

    accepted_idx = np.array(
        [fast.col_to_idx[c] for c in get_feature_columns(accepted_specs)],
        dtype=int,
    ) if accepted_specs else np.array([], dtype=int)

    baseline: dict[int, float] = {}
    for f in range(n_folds):
        m = scorer.score_fold(f, accepted_idx)
        if m is not None:
            baseline[f] = m
    if not baseline:
        raise ValueError("no scorable folds for the accepted set")

    # Fold-level shift infrastructure shared across families: offsets are
    # sampled once per (fold, replicate) and every family in that replicate
    # sees the same player-level scramble. This is load-bearing, not an
    # optimization: the acceptance bar takes the max null composite across
    # families per replicate, and a max over one joint null world per
    # replicate (Westfall-Young) is the honest "best of the fakes"; a max
    # over independently scrambled families would be inflated. Do not
    # un-share the draw.
    rng = np.random.default_rng(seed)
    uids = fast.row_uids.astype(str)
    fold_maps: list[list[tuple[np.ndarray, np.ndarray]]] = []
    for f in range(n_folds):
        train_idx, _ = fast.folds[f]
        maps_k = []
        for _rep in range(k):
            offsets = _sample_offsets(fast.row_player_ids, train_idx, rng)
            maps_k.append((
                _side_gather_map(
                    fast.row_player_ids, fast.row_dates, uids, train_idx,
                    offsets,
                ),
                _side_gather_map(
                    fast.row_opp_ids, fast.row_dates, uids, train_idx,
                    offsets,
                ),
            ))
        fold_maps.append(maps_k)

    def _one(family: str, member_specs: list[str]) -> FamilyNullVerdict:
        plan = resolve_rebuild(
            (rebuild_ids or {}).get(family, family), member_specs, fast.col_to_idx
        )
        if plan.missing:
            return FamilyNullVerdict(
                family=family, p_value=None,
                reason=f"unresolvable parents/members: {plan.missing[:6]}",
            )

        member_idx = np.array(
            [fast.col_to_idx[m.col] for m in plan.members], dtype=int
        )
        col_indices = np.concatenate([accepted_idx, member_idx])

        def _fold_gains(
            overrides_by_fold: list[dict[int, np.ndarray] | None],
        ) -> list[float]:
            gains = []
            for f, base_m in baseline.items():
                m = scorer.score_fold(f, col_indices, overrides_by_fold[f])
                if m is not None:
                    gains.append(sgn * (base_m - m))
            return gains

        observed_gains = _fold_gains([None] * n_folds)
        observed = composite_gain(observed_gains, min_agree)

        null_composites: list[float] = []
        for rep in range(k):
            overrides_by_fold: list[dict[int, np.ndarray] | None] = []
            for f in range(n_folds):
                p_map, o_map = fold_maps[f][rep]
                shifted: dict[int, np.ndarray] = {}
                for col in plan.shift_cols:
                    ci = fast.col_to_idx[col]
                    gmap = p_map if col.startswith("player_") else o_map
                    shifted[ci] = fast.X_wide[:, ci][gmap]
                overrides: dict[int, np.ndarray] = {}
                for member in plan.members:
                    ci = fast.col_to_idx[member.col]
                    if member.kind == "side":
                        if ci in shifted:
                            overrides[ci] = shifted[ci]
                        continue
                    li = fast.col_to_idx[member.left]
                    ri = fast.col_to_idx[member.right]
                    left = shifted.get(li, fast.X_wide[:, li])
                    right = shifted.get(ri, fast.X_wide[:, ri])
                    overrides[ci] = left + member.sign * right
                # Parents in the candidate set are member columns themselves
                # ("side" kind) and already overridden above; parents NOT in
                # the member list never reach the model, so shifting them
                # matters only through the rebuilt combiners.
                overrides_by_fold.append(overrides)
            null_composites.append(
                composite_gain(_fold_gains(overrides_by_fold), min_agree)
            )

        exceed = sum(1 for c in null_composites if c >= observed)
        return FamilyNullVerdict(
            family=family,
            p_value=(1 + exceed) / (k + 1),
            observed_composite=observed,
            observed_fold_gains=observed_gains,
            null_composites=null_composites,
        )

    key = _verdict_key(accepted_specs, k, seed, min_agree)
    # Only families in THIS call's set are restored: under top_m the tested
    # subset can shrink between attempts, and a stored verdict for a family
    # outside it must not be folded into the max-null.
    restored = {
        f: v for f, v in load_verdicts(checkpoint_path, key).items()
        if f in families
    }
    items = [it for it in sorted(families.items()) if it[0] not in restored]
    if restored:
        logger.info(
            "shifted null: restored %d/%d family verdicts from %s",
            len(restored), len(families), checkpoint_path.name,
        )
    t0 = time.perf_counter()
    verdicts: list[FamilyNullVerdict] = list(restored.values())
    fh = (
        open(checkpoint_path, "a", encoding="utf-8")
        if checkpoint_path is not None else None
    )

    def _collect(results) -> None:
        for fam_i, verdict in enumerate(results):
            verdicts.append(verdict)
            if fh is not None:
                _append_verdict(fh, verdict, key)
            if (fam_i + 1) % 10 == 0:
                logger.info(
                    "shifted null: %d/%d families in %.0fs",
                    fam_i + 1, len(items), time.perf_counter() - t0,
                )

    try:
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                _collect(ex.map(lambda it: _one(*it), items))
        else:
            _collect(_one(*it) for it in items)
    finally:
        if fh is not None:
            fh.close()
    verdicts.sort(key=lambda v: v.family)
    return verdicts


# ---------------------------------------------------------------------------
# Acceptance bars
# ---------------------------------------------------------------------------


def max_null(verdicts: list[FamilyNullVerdict]) -> list[float]:
    """Per replicate, the best null composite across every testable family —
    the round's "best of the fakes". Multiplicity lives in the max, so K
    replicates calibrate the test however many families were scored."""
    reps = [v.null_composites for v in verdicts if v.p_value is not None]
    if not reps:
        return []
    return [max(col) for col in zip(*reps)]


def max_null_p(observed: float, maxes: list[float]) -> float:
    """(1 + #replicates whose best fake >= observed) / (K + 1)."""
    exceed = sum(1 for m in maxes if m >= observed)
    return (1 + exceed) / (len(maxes) + 1)


def negative_control_floor(
    observed_gains: dict[str, float], min_pool: int = 40
) -> float | None:
    """95th percentile of the round's bottom-half families' observed gains —
    real features presumed null, carrying the correlation structure no
    synthetic null models. None when the pool is too small for a stable
    percentile (caller falls back to the shifted-candidate bar alone and
    says so in the run log)."""
    gains = sorted(observed_gains.values())
    bottom = gains[: len(gains) // 2]
    if len(bottom) < min_pool:
        return None
    return float(np.percentile(bottom, 95))


def make_family_acceptance(
    fast: FastForwardSelector,
    metric: str,
    families: dict[str, list[str]],
    cfg,
    direction: str = "minimize",
    _null_fn=run_family_nulls,
    workers: int = 1,
    n_jobs: int | None = None,
    checkpoint_path: Path | None = None,
):
    """Round-gate closure for ``FeatureSelector.acceptance_fn``: max-null
    test at ``cfg.alpha`` (bar a) intersected with the negative-control
    floor (bar b). ``cfg`` is a FamilyAcceptanceConfig; ``_null_fn`` is
    injectable so the gate logic tests engine-free. ``workers`` /
    ``n_jobs`` are the null runner's thread budget; ``checkpoint_path`` is
    the FS checkpoint, from which the per-round verdict log is derived."""

    def acceptance(
        selected_ids: list[str],
        best_metric: float,
        scores: dict[str, float],
        fold_scores: dict[str, list[float]],
    ) -> tuple[set[str], dict]:
        del fold_scores  # side-channel record; the bars recompute their own
        sgn = 1.0 if direction == "minimize" else -1.0
        gains = {f: sgn * (best_metric - s) for f, s in scores.items()}
        floor = negative_control_floor(gains, cfg.min_control_pool)
        if floor is None:
            logger.info(
                "family acceptance: control pool below %d families — "
                "negative-control floor unavailable, bar (a) alone applies",
                cfg.min_control_pool,
            )
        to_test = sorted(gains, key=gains.__getitem__, reverse=True)
        if cfg.top_m is not None and len(to_test) > cfg.top_m:
            logger.info(
                "family acceptance: null-testing top %d of %d families by "
                "observed gain (top_m cap) — the rest are ineligible this "
                "round, and the max-null is taken over the tested %d only, "
                "so alpha is family-wise over that subset, not the round "
                "(a weaker bar than the uncapped protocol)",
                cfg.top_m, len(to_test), cfg.top_m,
            )
            to_test = to_test[: cfg.top_m]
        accepted_specs = [
            c for fid in selected_ids for c in families.get(fid, [fid])
        ]
        verdicts = _null_fn(
            fast, metric, accepted_specs,
            {f: families[f] for f in to_test if f in families},
            k=cfg.k,
            seed=cfg.seed + len(selected_ids),  # fresh nulls each round
            min_agree=cfg.min_agree,
            direction=direction,
            workers=workers,
            n_jobs=n_jobs,
            # same round numbering as the selector: seeds count as selected
            checkpoint_path=null_verdicts_path(
                checkpoint_path, len(selected_ids) + 1
            ),
        )
        testable = [v for v in verdicts if v.p_value is not None]
        untestable = sorted(v.family for v in verdicts if v.p_value is None)
        maxes = max_null(testable)
        # A -inf max means every tested family's fake failed the
        # fold-agreement gate in that replicate: any family clearing the
        # gate beats it regardless of magnitude. Negligible at full pool
        # size, live under top_m or a thinned pool — so it is counted.
        neg_inf = sum(1 for m in maxes if m == float("-inf"))
        if neg_inf:
            logger.warning(
                "family acceptance: best fake failed the fold-agreement "
                "gate in %d/%d replicates — bar (a) does not rank magnitude "
                "there",
                neg_inf, len(maxes),
            )
        p_max = {
            v.family: max_null_p(v.observed_composite, maxes) for v in testable
        }
        bar_a = {f for f, p in p_max.items() if p <= cfg.alpha}
        eligible = (
            bar_a if floor is None
            else {f for f in bar_a if gains[f] >= floor}
        )
        info = {
            "max_null": maxes,
            "max_null_neg_inf": neg_inf,
            "p_max": p_max,
            # own-null p-values: diagnostic only (see module docstring)
            "p_own": {v.family: v.p_value for v in testable},
            "untestable": untestable,
            "control_floor": floor,
            "bar_a": sorted(bar_a),
            "eligible": sorted(eligible),
            "tested": len(to_test),
            "scored": len(scores),
        }
        return eligible, info

    return acceptance


# ---------------------------------------------------------------------------
# Within-family pick: family is the unit of evidence, columns the unit of
# deployment
# ---------------------------------------------------------------------------


def refine_family(
    fast: FastForwardSelector,
    metric: str,
    accepted_specs: list[str],
    family: str,
    members: list[str],
    cfg,
    direction: str = "minimize",
    seed: int = 0,
    workers: int = 1,
    n_jobs: int | None = None,
    _null_fn=run_family_nulls,
) -> tuple[list[str], dict]:
    """Reduce an accepted family to the members that earn their place.

    Greedy over the family's members, each tested as a one-column candidate
    against the family's own max-null (the best of the members' shifted
    copies, per replicate): take the best member with p <= ``cfg.alpha``,
    condition on it, repeat, up to ``cfg.max_members``. Fifteen candidates
    instead of ~4,900, so the within-family max is a small optimism and the
    null prices it.

    If no member clears on its own while the block did, the signal is spread
    across windows/forms and the block is kept whole — that branch is what
    keeps corroboration from silently collapsing back to column-greedy.
    """
    picked: list[str] = []
    passes: list[dict] = []
    cap = cfg.max_members if cfg.max_members is not None else len(members)
    while len(picked) < cap:
        cands = [m for m in members if m not in picked]
        if not cands:
            break
        verdicts = _null_fn(
            fast, metric, accepted_specs + picked, {m: [m] for m in cands},
            k=cfg.k, seed=seed + len(picked), min_agree=cfg.min_agree,
            direction=direction, workers=workers, n_jobs=n_jobs,
            rebuild_ids={m: family for m in cands},
        )
        testable = [v for v in verdicts if v.p_value is not None]
        maxes = max_null(testable)
        p_max = {v.family: max_null_p(v.observed_composite, maxes) for v in testable}
        eligible = [v for v in testable if p_max[v.family] <= cfg.alpha]
        best = max(eligible, key=lambda v: v.observed_composite, default=None)
        passes.append({
            "given": list(picked),
            "max_null": maxes,
            "p_max": p_max,
            "observed": {v.family: v.observed_composite for v in testable},
            "untestable": sorted(v.family for v in verdicts if v.p_value is None),
            "picked": best.family if best is not None else None,
        })
        if best is None:
            break
        picked.append(best.family)

    if not picked:
        logger.info(
            "    %s: no member clears alone; the block (%d columns) stays",
            family, len(members),
        )
        return list(members), {"resolved": "block", "members": list(members), "passes": passes}
    logger.info("    %s -> %s", family, ", ".join(picked))
    return picked, {"resolved": "members", "members": picked, "passes": passes}


def make_family_refiner(
    fast: FastForwardSelector,
    metric: str,
    families: dict[str, list[str]],
    cfg,
    direction: str = "minimize",
    workers: int = 1,
    n_jobs: int | None = None,
    _refine_fn=refine_family,
):
    """Post-acceptance hook for ``FeatureSelector.refine_fn``. ``families``
    is the selector's own (shared) mapping, so accepted families already
    reduced by earlier rounds expand to their kept members here."""

    def refine(family: str, selected_ids: list[str]) -> tuple[list[str], dict]:
        accepted_specs = [
            c for fid in selected_ids if fid != family
            for c in families.get(fid, [fid])
        ]
        return _refine_fn(
            fast, metric, accepted_specs, family, list(families[family]), cfg,
            direction=direction, seed=cfg.seed + 1000 + len(selected_ids),
            workers=workers, n_jobs=n_jobs,
        )

    return refine
