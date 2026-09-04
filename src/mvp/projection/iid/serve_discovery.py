"""Forward-selection discovery for the score-state serve model.

Caches all match-level candidate features to disk once, then iteratively adds
the candidate feature whose inclusion most improves the CV metric. Match-level
candidates are loaded lazily from cache one at a time — only the
currently-evaluated feature is joined to the base point-grain matrix, keeping
peak memory proportional to (rows × |selected| + |point_features|) rather than
(rows × |pool|). After FS terminates, optionally re-trains all configured
`model_forms` on the final feature set for comparison.
"""

import copy
import gc
import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.discovery.checkpoint import (
    SelectionCheckpoint,
    load_checkpoint,
    save_checkpoint,
)
from mvp.model.engine import (
    FeatureEngine,
    build_column_name,
    check_memory,
    make_fs_engine,
    parse_feature_spec,
    process_rss_mb,
    system_mem_used_pct,
)
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.metrics import compute_metrics
from mvp.model.splitters import make_splitter
from mvp.model.parallelism import blas_thread_cap, resolve_candidate_parallelism
from mvp.model.discovery.discover import get_all_feature_specs
from mvp.model.discovery.selection import (
    _append_fs_history,
    _fs_history_path,
    _fs_progress_path,
)
from mvp.projection.iid.config import ServeDiscoveryConfig
from mvp.projection.iid.metric_registry import (
    base_metric_of,
    is_branch_metric,
    is_minimize,
    needs_match_grain_prep,
    score_chain,
    worst_score,
)
from mvp.projection.iid.score_state_features import (
    add_derived_point_features,
    default_point_level_candidate_pool,
)
from mvp.projection.iid.score_state_model import build_score_state_model
from mvp.projection.iid.serve_model import (
    ScoreStateChainServeModel,
    swap_side_partner_specs,
)
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn
from mvp.projection.iid.two_level_serve_model import (
    FIRST_IN,
    TwoLevelServeModel,
    _ConstantBranch,
    first_in_counts,
)

logger = logging.getLogger(__name__)


def live_partial_scores(
    restored: dict[str, float],
    candidate_match: list[str],
    candidate_point: list[str],
) -> dict[str, float]:
    """Restored mid-round scores, minus candidates the pool no longer offers.

    The candidate pool is rebuilt from the config on every run, so trimming the
    candidate list and resuming is a supported edit — the whole point of the
    frozen config living in the run dir. The checkpoint, though, carries a score
    for every candidate the interrupted round had reached, and the round's
    leader can be one of the ones just removed. Unfiltered, the round then
    selects a feature that is not in the pool and the run dies at the point of
    selection.

    The completed-rounds replay guards the same hazard with `if feat in
    candidate_*`; this path had no equivalent. Dropping a removed candidate's
    score is the whole fix: it simply is not eligible to win the round.
    """
    live = set(candidate_match) | set(candidate_point)
    return {k: v for k, v in restored.items() if k in live}

# Point features the chain cannot represent. Two distinct mechanisms:
#
# `point_num` — the deuce closed-form in stateful_chain.hold_from_state_fn
# treats ("D","D") as a single absorbing node, so a feature that advances
# through deuce cycles (point_num resets per game but keeps counting through
# deuce) would invalidate the closed form.
#
# `serve` / `is_second_serve` — the chain has no serve tree. A point is atomic,
# and every ScoreState it builds hardcodes serve_num=1 (stateful_chain.py:206,
# 218, 322, 332; serve_model.py:67). A model given serve number learns
# P(win | 1st in) ≈ 0.69 separately from P(win | 2nd) ≈ 0.50, then is only ever
# asked for the first — so the chain receives ~0.69 where it needs the blended
# ~0.62, holds become near-certain and total games collapse. Measured in FS
# round 1: CRPS 3.910 for both, against ~3.373 for every other candidate and
# 3.366 base-only. They ranked last there, which is luck rather than
# protection — nothing stopped them being selected.
#
# The fix for wanting serve-conditional behaviour is the two-level estimator
# (mvp-docs/specs/2026-08-10-two-level-point-model.md), which composes the
# branches into a single marginal `p` BEFORE the chain sees it. That makes
# serve number a conditioner rather than a feature, and it does not change
# this exclusion: within each branch's rows the flag is constant anyway.
_CHAIN_INCOMPATIBLE_POINT_FEATURES = frozenset(
    {"point_num", "serve", "is_second_serve"}
)


def _branch_point_columns() -> list[str]:
    """Columns a win branch's held-out point frame must carry.

    The UNION over the point candidate pool, not one candidate's list: the
    frame is built once per fold and every candidate selects a different
    subset out of it. Keys, every raw point feature that can be a candidate,
    and the source columns the derived features read -- the latter taken from
    the expressions themselves (`meta.root_names()`) rather than restated, so
    a derivation that starts reading a new column cannot silently fall out of
    the narrowing and surface as a ColumnNotFound deep inside the join.
    """
    from mvp.projection.iid.score_state_features import (
        DERIVED_POINT_FEATURES,
        RAW_POINT_FEATURES,
    )

    keys = ["match_uid", "server_id", "returner_id", "serve", "point_won_by_server"]
    sources: set[str] = set()
    for build in DERIVED_POINT_FEATURES.values():
        sources.update(build().meta.root_names())
    out: list[str] = []
    for c in keys + list(RAW_POINT_FEATURES) + sorted(sources):
        if c not in out:
            out.append(c)
    return out


@dataclass
class FSRoundResult:
    round_idx: int
    feature_added: str | None
    grain: str  # "match" or "point"
    score: float  # metric value (lower is better for log_loss / brier)
    delta: float  # improvement over previous round
    selected_match_level: list[str] = field(default_factory=list)
    selected_point_level: list[str] = field(default_factory=list)


@dataclass
class DiscoveryResult:
    selected_match_level: list[str]
    selected_point_level: list[str]
    rounds: list[FSRoundResult]
    n_train_rows: int


@dataclass(frozen=True)
class _ChainFold:
    """One fold's fit/score inputs for the chain-metric path, built once.

    Everything here depends only on the fold — not on which candidate is being
    scored — so it is materialized in `_prepare_match_data` rather than in
    `_score_cv_chain`. Same reasoning as the classification FS
    (`fast_selection._compute_fold_margins`): work that varies only by fold, done
    per candidate, repeats identical work thousands of times per round. Here that
    was a `is_in` filter over the full preloaded points frame (~5M rows) plus two
    row-gathers of the wide match frame, once per candidate per fold.

    SHARED ACROSS THREADS. `_score_cv_chain` runs under a ThreadPoolExecutor with
    `n_parallel_candidates` workers, which previously each built private copies of
    these frames and now all read these. That is safe because every consumer path
    is non-mutating — `select` / `rename` / `join` / `filter` / `with_columns` all
    return new frames, and each `to_numpy()` on them is either a multi-column
    `select().to_numpy()` or followed by `.astype()`, both of which copy. The
    frames must stay read-only: an in-place mutation added downstream would become
    a data race here, which is why this type is frozen and why nothing hands out a
    writable view.

    Held materialized rather than as row indices because the consumers
    (`ServeWinProbEstimator.fit` / `.predict_state_fn`) take DataFrames; the
    classification FS can keep row-aligned numpy and slice at use, this path
    cannot.

    That divergence has a cost worth naming. `projection/lines/fast_selection.py`
    (`create_scorer`) gathers rows AND the candidate's columns together —
    `X_wide[np.ix_(train_idx, col_indices)]` — so nothing wide is ever held per
    fold. Here the frames are full-width, so under an empty
    `candidate_match_level_features` (→ the full registered pool) each fold slice
    carries every candidate column, while `ScoreStateChainServeModel.fit` reads
    only the ~20 its candidate names. Pre-narrowing would not help: the column
    subset changes with every candidate, so a narrowed cache would have to hold
    their union, which is the whole pool again. Narrowing per candidate instead is
    not free to replicate — the server_/returner_ and opp_-mirror resolution lives
    in the estimator (which does its own `select`), and restating it here is the
    drift `_build_candidate_model` already warns against for `_STATE_DERIVABLE`.
    `fs_match_subsample` is the intended lever: it caps training matches per fold,
    which is what bounds these slices. Measured on the 2023-2025 tour+chal bo3
    window with the full pool — 13.16 GB held here with the cap unset, 6.37 GB at
    `fs_match_subsample=5000`, against a 13.70 GB peak for the churn it replaces.
    """

    train_df: pl.DataFrame
    test_df: pl.DataFrame
    feats: pl.DataFrame
    points: pl.DataFrame
    # HELD-OUT inputs, materialized only for a branch-metric run (plan:
    # 2026-09-03-per-branch-selection-metrics). The chain scores on
    # `test_df` through the composed distribution and needs neither.
    #
    # `test_points` is the win branches' test-side point frame, narrowed to
    # the UNION over the whole point candidate pool -- the frame is built
    # once and the candidate's point list changes per fit, so per-candidate
    # narrowing is not available. `test_first_in` is the (match, server)
    # first-serve aggregate, built to match FirstServeInModel._aggregate
    # exactly (pl.len() denominator, no target filter).
    test_points: pl.DataFrame | None = None
    test_first_in: pl.DataFrame | None = None


class ServeDiscoverySelector:
    """Forward-selection orchestrator for the score-state serve model."""

    def __init__(
        self,
        config_path: Path | str,
        *,
        points_path: Path | str | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        checkpoint_path: Path | str | None = None,
        run_name: str | None = None,
        checkpoint_interval: int = 25,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = ServeDiscoveryConfig.from_file(config_path)
        self.points_path = Path(points_path) if points_path else get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        self.matches_path = Path(matches_path) if matches_path else get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        self.cache_dir = Path(cache_dir) if cache_dir else get_local_data_root() / "features" / "cache"
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.run_name = run_name or self.config_path.stem
        self.checkpoint_interval = checkpoint_interval
        # Two-level component runs: the two FIXED components fitted once per
        # fold (plan 2026-09-03-serve-fs-wall-time, Phase 1). None for
        # single-level runs. Keyed by fold index.
        self._prefit_fixed: dict[int, dict[str, Any]] | None = None
        # Per-phase wall-time accumulated across candidate-folds since the
        # last [diag] line (Phase 0). Written from candidate threads.
        self._phase_totals: dict[str, float] = {}
        self._phase_cand_folds: int = 0
        self._phase_lock = threading.Lock()

        # Match-grain cache for chain-metric path (lazily populated).
        self._match_df: pl.DataFrame | None = None
        self._match_splits: list[tuple[list[int], list[int]]] | None = None
        # Subsampled version of _match_splits for FS candidate scoring (fs_match_subsample).
        # Equals _match_splits when fs_match_subsample is None.
        self._fs_match_splits: list[tuple[list[int], list[int]]] | None = None
        # Pre-loaded data for chain-path model.fit() — avoids reading full
        # matches.parquet (1.67M rows) and points.parquet (7.1M rows) per candidate per fold.
        self._match_features_both_sides: pl.DataFrame | None = None
        self._preloaded_points: pl.DataFrame | None = None
        # Per-fold fit/score inputs, built once from _fs_match_splits at the end
        # of _prepare_match_data. See _ChainFold.
        self._chain_folds: list[_ChainFold] | None = None
        # FeatureEngine shared across chain-path fits — set in run() after
        # _pre_cache_all. Reusing one instance keeps cache_key stable even if
        # matches.parquet is touched mid-run.
        self._engine: FeatureEngine | None = None

    def run(self) -> DiscoveryResult:
        selected_match = list(self.config.features.base_match_level_features)
        selected_point = list(self.config.features.base_point_level_features)
        # Empty candidate list → full pool, matching classification / projection / IID FS.
        match_pool = list(self.config.features.candidate_match_level_features)
        if not match_pool:
            match_pool = get_all_feature_specs(window_sizes=self.config.features.window_sizes)
            logger.info("candidate_match_level_features empty → using full registered pool (%d specs)", len(match_pool))
        point_pool = list(self.config.features.candidate_point_level_features)
        if not point_pool:
            point_pool = default_point_level_candidate_pool()
            logger.info("candidate_point_level_features empty → using full default pool (%d specs)", len(point_pool))

        match_excludes = set(self.config.features.exclude_match_level_features)
        if match_excludes:
            unknown = match_excludes - set(match_pool)
            if unknown:
                raise ValueError(
                    f"exclude_match_level_features contains specs not in the candidate pool: "
                    f"{sorted(unknown)}"
                )
            base_conflict = match_excludes & set(selected_match)
            if base_conflict:
                raise ValueError(
                    f"exclude_match_level_features overlaps base_match_level_features: "
                    f"{sorted(base_conflict)}"
                )
            before = len(match_pool)
            match_pool = [f for f in match_pool if f not in match_excludes]
            logger.info("Excluded %d match-level specs (pool %d → %d)", before - len(match_pool), before, len(match_pool))

        point_excludes = set(self.config.features.exclude_point_level_features)
        if point_excludes:
            unknown = point_excludes - set(point_pool)
            if unknown:
                raise ValueError(
                    f"exclude_point_level_features contains specs not in the candidate pool: "
                    f"{sorted(unknown)}"
                )
            base_conflict = point_excludes & set(selected_point)
            if base_conflict:
                raise ValueError(
                    f"exclude_point_level_features overlaps base_point_level_features: "
                    f"{sorted(base_conflict)}"
                )
            before = len(point_pool)
            point_pool = [f for f in point_pool if f not in point_excludes]
            logger.info("Excluded %d point-level specs (pool %d → %d)", before - len(point_pool), before, len(point_pool))

        # Excluded for a BRANCH metric too, though nothing in branch scoring
        # would break: the endpoint is a chain config, and within one branch
        # `serve` / `is_second_serve` are constant, so a branch metric would
        # rank them as harmless noise while the chain builds every ScoreState
        # with serve_num=1.
        if needs_match_grain_prep(self.config.metric):
            dropped = [f for f in point_pool if f in _CHAIN_INCOMPATIBLE_POINT_FEATURES]
            if dropped:
                point_pool = [f for f in point_pool if f not in _CHAIN_INCOMPATIBLE_POINT_FEATURES]
                logger.info(
                    "Metric %s: excluding point features incompatible with the deuce closed-form: %s",
                    self.config.metric, dropped,
                )

        # Phase A: cache all match-level specs to disk (memory-bounded batches).
        # Phase B: build the point-grain base matrix with only base match features.
        # Match-level candidates are loaded lazily one at a time during FS.
        engine, cache_key = self._pre_cache_all(base_match=selected_match, candidate_match=match_pool)
        self._engine = engine
        base_df, slim_matches = self._build_base_matrix(
            engine, cache_key,
            base_match=selected_match, base_point=selected_point, candidate_point=point_pool,
        )
        logger.info("Base matrix: %d rows, %d columns", len(base_df), len(base_df.columns))

        splitter = self._make_splitter()
        splits = list(splitter.split(base_df))
        logger.info("FS splits: %d folds", len(splits))

        fs_splits = self._maybe_subsample_splits(splits)

        # Chain and branch metrics both need a match-grain df with all candidate
        # match features materialized, plus match-grain folds from
        # config.validation. Branch scoring skips the chain, not the prep.
        if needs_match_grain_prep(self.config.metric):
            self._prepare_match_data(match_pool=match_pool, engine=engine, cache_key=cache_key)

        candidate_match = [c for c in match_pool if c not in selected_match]
        candidate_point = [c for c in point_pool if c not in selected_point]
        first_round_logged = False

        # Attempt to restore from checkpoint
        cp = self._load_checkpoint() if self.checkpoint_path else None

        # Append-only per-round ranking log, sibling of the checkpoint. Unlike
        # the checkpoint it survives run completion, so the full per-round
        # candidate scores stay queryable afterwards. A fresh run drops any
        # stale file so rounds from a previous run cannot be read as this one's
        # (selection.py:308-312 does the same).
        history_path = _fs_history_path(self.checkpoint_path)
        # Live human-readable selected list, rewritten each round so it can be
        # cat-ed mid-run. Same sibling-of-checkpoint convention as the
        # classification / lines path.
        progress_path = _fs_progress_path(self.checkpoint_path)
        if cp is None and history_path is not None and history_path.exists():
            history_path.unlink()

        rounds: list[FSRoundResult] = []
        partial_round_scores: dict[str, float] = {}

        if cp is not None:
            # Replay completed rounds — extend base_df with any restored match features.
            for entry in cp.completed_rounds:
                feat = entry["feature"]
                grain = entry["grain"]
                score = entry["score"]
                if grain == "match":
                    selected_match.append(feat)
                    if feat in candidate_match:
                        candidate_match.remove(feat)
                    if not needs_match_grain_prep(self.config.metric):
                        base_df = self._extend_df_with_match_feature(base_df, slim_matches, engine, cache_key, feat)
                else:
                    selected_point.append(feat)
                    if feat in candidate_point:
                        candidate_point.remove(feat)
                rounds.append(
                    FSRoundResult(
                        # +1 because the fresh path (below) puts a
                        # feature_added=None sentinel at index 0 and starts
                        # selecting at round 1, while the checkpoint stores only
                        # {feature, grain, score} — no index, and no sentinel.
                        # Without the offset every restored round comes back
                        # numbered one lower than it was, and `cp.current_round`
                        # then resumes the original counter, so the progress file
                        # shows a one-round gap at the resume boundary.
                        round_idx=len(rounds) + 1,
                        feature_added=feat,
                        grain=grain,
                        score=score,
                        delta=0.0,  # not retained in checkpoint
                        selected_match_level=list(selected_match),
                        selected_point_level=list(selected_point),
                    )
                )
            current_score = cp.best_metric
            round_idx = cp.current_round
            partial_round_scores = live_partial_scores(
                cp.current_round_scores, candidate_match, candidate_point
            )
            dropped = len(cp.current_round_scores) - len(partial_round_scores)
            logger.info(
                "Resumed from checkpoint: %d completed rounds, current score=%.6f, partial scores for %d candidates",
                len(rounds), current_score, len(partial_round_scores),
            )
            if dropped:
                logger.warning(
                    "Dropped %d restored round-%d score(s) for candidates no "
                    "longer in the pool; they will not be selected this round",
                    dropped, round_idx,
                )
        else:
            if selected_match or selected_point:
                current_score = self._score_cv(base_df, fs_splits, selected_match, selected_point)
                logger.info("Base-only CV %s = %.6f (%d features)", self.config.metric, current_score, len(selected_match) + len(selected_point))
            else:
                current_score = worst_score(self.config.metric)
                logger.info("No base features — starting from worst-case score")
            rounds.append(
                FSRoundResult(
                    round_idx=0,
                    feature_added=None,
                    grain="base",
                    score=current_score,
                    delta=0.0,
                    selected_match_level=list(selected_match),
                    selected_point_level=list(selected_point),
                )
            )
            round_idx = 1

        started_at = cp.started_at if cp else datetime.now()

        # Silence per-candidate engine / serve-model chatter during the FS
        # loop — it clobbers the tqdm bar and dwarfs the useful round-level
        # lines this module emits.
        noisy_loggers = [
            logging.getLogger("mvp.model.engine"),
            logging.getLogger("mvp.projection.iid.serve_model"),
        ]
        prev_levels = [(lg, lg.level) for lg in noisy_loggers]
        for lg in noisy_loggers:
            lg.setLevel(logging.WARNING)

        while True:
            if self.config.features.max_features is not None:
                n_total = len(selected_match) + len(selected_point)
                if n_total >= self.config.features.max_features:
                    break
            if not candidate_match and not candidate_point:
                break

            from tqdm import tqdm

            best_new_score = current_score
            best_cand: str | None = None
            best_grain: str | None = None

            tagged = [("match", c) for c in candidate_match] + [("point", c) for c in candidate_point]
            total_cands = len(tagged)
            cap = self.config.features.max_features
            # Each round adds at most one feature — show the target count
            # this round is aiming for.
            target_total = len(selected_match) + len(selected_point) + 1
            desc = f"Round {round_idx}" + (f" ({target_total}/{cap})" if cap else "")

            this_round_scores: dict[str, float] = dict(partial_round_scores)
            partial_round_scores = {}

            # Seed best from partial scores if any (before creating tqdm so log
            # lines don't interleave with the progress bar).
            if this_round_scores:
                best_prev_cand = min(this_round_scores, key=this_round_scores.get) if is_minimize(self.config.metric) else max(this_round_scores, key=this_round_scores.get)
                cand_score = this_round_scores[best_prev_cand]
                if self._is_better(cand_score, best_new_score):
                    best_new_score = cand_score
                    best_cand = best_prev_cand
                    best_grain = "match" if best_prev_cand in candidate_match else "point"
                logger.info(
                    "  Restored %d/%d candidate scores from checkpoint",
                    len(this_round_scores), total_cands,
                )

            eval_count = 0
            # Both scoring routes that use the match-grain folds; `chain_mode`
            # gates the parallel loop and the base_df extend, not the chain
            # itself (see `_score_cv`'s dispatch).
            chain_mode = needs_match_grain_prep(self.config.metric)

            if chain_mode and self.config.n_parallel_candidates > 1:
                to_score = [(g, c) for g, c in tagged if c not in this_round_scores]

                # Unlike the classification FS (threads/fit auto-derived to ~4),
                # candidates and per-fit n_jobs are independent knobs here that
                # multiply — so show both: (candidates x n_jobs).
                per_fit = self.config.scoring_model.params.get("n_jobs", 1)
                bar = tqdm(total=len(tagged), initial=len(this_round_scores),
                           desc=f"{desc} ({self.config.n_parallel_candidates}x{per_fit})",
                           leave=False, ncols=120)
                if best_cand is not None and hasattr(bar, "set_postfix"):
                    bar.set_postfix(
                        best=f"{best_new_score:.6f}",
                        feat=f"{best_cand}[{best_grain}]",
                        refresh=False,
                    )

                def _score_one(grain_cand, _ml=selected_match, _pl=selected_point):
                    g, c = grain_cand
                    ml = _ml + [c] if g == "match" else list(_ml)
                    pl_feats = list(_pl) if g == "match" else _pl + [c]
                    return g, c, self._score_cv_match_grain(ml, pl_feats)

                # BLAS thread cap for a logistic scorer (whose fit ignores n_jobs)
                # so concurrent worker fits don't oversubscribe; no-op for xgboost,
                # which routes n_jobs through OpenMP. Set once around this round's
                # executor, not per-fit (process-global BLAS limit would race).
                _, cap_n_jobs = resolve_candidate_parallelism(
                    self.config.scoring_model.params.get("n_jobs"),
                    self.config.n_parallel_candidates,
                )
                last_log_t = time.perf_counter()
                last_log_eval = 0
                with (
                    blas_thread_cap(self.config.scoring_model.type, cap_n_jobs),
                    ThreadPoolExecutor(
                        max_workers=self.config.n_parallel_candidates
                    ) as executor,
                ):
                    futures = {executor.submit(_score_one, gc): gc for gc in to_score}
                    for future in as_completed(futures):
                        grain, cand, score = future.result()
                        this_round_scores[cand] = score
                        eval_count += 1
                        bar.update(1)
                        if self._is_better(score, best_new_score):
                            best_new_score = score
                            best_cand = cand
                            best_grain = grain
                            if hasattr(bar, "set_postfix"):
                                bar.set_postfix(
                                    best=f"{best_new_score:.6f}",
                                    feat=f"{cand}[{grain}]",
                                    refresh=False,
                                )
                        if (
                            self.checkpoint_path
                            and self.checkpoint_interval > 0
                            and eval_count % self.checkpoint_interval == 0
                        ):
                            now = time.perf_counter()
                            n_since = eval_count - last_log_eval
                            sec_per_it = (now - last_log_t) / max(n_since, 1)
                            rss = process_rss_mb()
                            sys_pct = system_mem_used_pct()
                            logger.info(
                                "  [diag] round %d eval=%d/%d s/it=%.2f rss=%s sys=%s %s",
                                round_idx, eval_count, len(to_score), sec_per_it,
                                f"{rss:.0f}MB" if rss is not None else "n/a",
                                f"{sys_pct}%" if sys_pct is not None else "n/a",
                                self._format_phases(self._take_phase_means()),
                            )
                            last_log_t = now
                            last_log_eval = eval_count
                            self._save_checkpoint(
                                started_at=started_at,
                                completed_rounds=[
                                    {"feature": r.feature_added, "grain": r.grain, "score": r.score}
                                    for r in rounds if r.feature_added is not None
                                ],
                                current_round=round_idx,
                                total_candidates=total_cands,
                                current_round_scores=this_round_scores,
                                best_metric=current_score,
                            )
                bar.close()
            else:
                bar = tqdm(tagged, desc=desc, leave=False, ncols=120)
                if best_cand is not None and hasattr(bar, "set_postfix"):
                    bar.set_postfix(
                        best=f"{best_new_score:.6f}",
                        feat=f"{best_cand}[{best_grain}]",
                        refresh=False,
                    )
                last_log_t = time.perf_counter()
                last_log_eval = 0
                for grain, cand in bar:
                    if cand in this_round_scores:
                        score = this_round_scores[cand]
                    else:
                        if grain == "match":
                            # Chain path ignores the extended point-grain df — it
                            # scores off self._match_df which already has every
                            # candidate match feature materialized. Skip the extend.
                            if chain_mode:
                                score = self._score_cv(base_df, fs_splits, selected_match + [cand], selected_point)
                            else:
                                extended = self._extend_df_with_match_feature(base_df, slim_matches, engine, cache_key, cand)
                                score = self._score_cv(extended, fs_splits, selected_match + [cand], selected_point)
                        else:
                            score = self._score_cv(base_df, fs_splits, selected_match, selected_point + [cand])
                        this_round_scores[cand] = score
                        eval_count += 1
                        if (
                            self.checkpoint_path
                            and self.checkpoint_interval > 0
                            and eval_count % self.checkpoint_interval == 0
                        ):
                            now = time.perf_counter()
                            n_since = eval_count - last_log_eval
                            sec_per_it = (now - last_log_t) / max(n_since, 1)
                            rss = process_rss_mb()
                            sys_pct = system_mem_used_pct()
                            logger.info(
                                "  [diag] round %d eval=%d/%d s/it=%.2f rss=%s sys=%s %s",
                                round_idx, eval_count, len(tagged), sec_per_it,
                                f"{rss:.0f}MB" if rss is not None else "n/a",
                                f"{sys_pct}%" if sys_pct is not None else "n/a",
                                self._format_phases(self._take_phase_means()),
                            )
                            last_log_t = now
                            last_log_eval = eval_count
                            self._save_checkpoint(
                                started_at=started_at,
                                completed_rounds=[
                                    {"feature": r.feature_added, "grain": r.grain, "score": r.score}
                                    for r in rounds if r.feature_added is not None
                                ],
                                current_round=round_idx,
                                total_candidates=total_cands,
                                current_round_scores=this_round_scores,
                                best_metric=current_score,
                            )
                    if self._is_better(score, best_new_score):
                        best_new_score = score
                        best_cand = cand
                        best_grain = grain
                        if hasattr(bar, "set_postfix"):
                            bar.set_postfix(best=f"{best_new_score:.6f}", feat=f"{cand}[{grain}]", refresh=False)

            best_delta = (
                self._improvement(current_score, best_new_score)
                if best_cand is not None
                else -math.inf
            )
            if best_cand is None or best_delta < self.config.min_delta:
                reason = (
                    "no candidate produced a finite score" if best_cand is None
                    else f"improvement {best_delta:.6f} < min_delta {self.config.min_delta:.6f}"
                )
                logger.info("FS halting: %s", reason)
                # The halt round's ranking is the only record of what nearly
                # cleared the bar — same `stop` record shape as the
                # classification path (selection.py) so one reader serves both.
                _append_fs_history(history_path, self._stop_record(
                    round_idx, reason, current_score, best_cand, best_new_score,
                    this_round_scores, is_minimize(self.config.metric),
                ))
                break

            if best_grain == "match":
                selected_match.append(best_cand)
                candidate_match.remove(best_cand)
                # Chain path doesn't use base_df for scoring — skip the extend.
                if not chain_mode:
                    base_df = self._extend_df_with_match_feature(base_df, slim_matches, engine, cache_key, best_cand)
            else:
                selected_point.append(best_cand)
                candidate_point.remove(best_cand)
            current_score = best_new_score
            logger.info(
                "Round %d: +%s [%s] → %s=%.6f (Δ=%.6f)",
                round_idx, best_cand, best_grain, self.config.metric, current_score, best_delta,
            )
            rounds.append(
                FSRoundResult(
                    round_idx=round_idx,
                    feature_added=best_cand,
                    grain=best_grain,
                    score=current_score,
                    delta=best_delta,
                    selected_match_level=list(selected_match),
                    selected_point_level=list(selected_point),
                )
            )

            # Durable per-round ranking, same file shape as the classification /
            # lines path (selection.py:523-529) so one reader serves both.
            #
            # Not optional bookkeeping: the RUNNER-UP of a round is the only
            # record of what the search would have taken had the winner been
            # unavailable, and it is what makes a pool decision reviewable after
            # the fact. Without it a run that selects a feature you later decide
            # to exclude leaves nothing behind — the round has to be re-run to
            # learn what was second. That is exactly what happened on base_ol
            # (2026-08-18): round 9 took a serve-specific _diff whose opponent
            # term carries no causal load for a serve estimand, and the
            # alternative it beat was unrecoverable.
            #
            # Non-finite scores are dropped rather than sorted, matching the
            # round-1 console block below; the count is kept so a round that
            # rejected many candidates is still legible.
            ranked_round = sorted(
                ((f, m) for f, m in this_round_scores.items() if math.isfinite(m)),
                key=lambda x: x[1],
                reverse=not is_minimize(self.config.metric),
            )
            # Selection ORDER, not grain-grouped: `rounds` is the interleaved
            # record of what the search actually took and when, which grouping by
            # grain destroys. Pinned base features precede it unnumbered — they
            # were given, not chosen, and numbering them alongside the rounds
            # would imply a search order they never had.
            progress_path.write_text(
                chr(10).join(
                    [f"   base. {f} [match]" for f in
                     self.config.features.base_match_level_features]
                    + [f"   base. {f} [point]" for f in
                       self.config.features.base_point_level_features]
                    + [f"{r.round_idx:>7}. {r.feature_added} [{r.grain}]"
                       for r in rounds if r.feature_added is not None]
                ),
                encoding="utf-8",
            )
            _append_fs_history(history_path, {
                "round": round_idx,
                "action": "add",
                "feature": best_cand,
                "grain": best_grain,
                "metric": current_score,
                "delta": best_delta,
                "n_non_finite": len(this_round_scores) - len(ranked_round),
                "ranking": ranked_round,
            })

            if not first_round_logged and round_idx == 1:
                first_round_logged = True
                reverse = not is_minimize(self.config.metric)
                ranked = [
                    (f, m) for f, m in this_round_scores.items() if math.isfinite(m)
                ]
                ranked.sort(key=lambda x: x[1], reverse=reverse)
                n_dropped = len(this_round_scores) - len(ranked)
                logger.info("")
                logger.info("ROUND 1 FEATURE RANKING (%d candidates)", len(ranked))
                logger.info("-" * 50)
                for i, (feat, metric) in enumerate(ranked, 1):
                    logger.info("  %3d. %s: %.6f", i, feat, metric)
                if n_dropped:
                    logger.info("  (%d features rejected / returned non-finite)", n_dropped)

            round_idx += 1
            # Commit round to checkpoint (with empty current_round_scores)
            if self.checkpoint_path:
                self._save_checkpoint(
                    started_at=started_at,
                    completed_rounds=[
                        {"feature": r.feature_added, "grain": r.grain, "score": r.score}
                        for r in rounds if r.feature_added is not None
                    ],
                    current_round=round_idx,
                    total_candidates=0,
                    current_round_scores={},
                    best_metric=current_score,
                )

        # FS complete — remove checkpoint (final-form eval is cheap, no need to checkpoint it).
        if self.checkpoint_path and self.checkpoint_path.exists():
            self.checkpoint_path.unlink()

        # Restore log levels silenced around the FS.
        for lg, lvl in prev_levels:
            lg.setLevel(lvl)

        return DiscoveryResult(
            selected_match_level=selected_match,
            selected_point_level=selected_point,
            rounds=rounds,
            n_train_rows=len(base_df),
        )

    def _load_checkpoint(self) -> SelectionCheckpoint | None:
        if self.checkpoint_path is None:
            return None
        return load_checkpoint(self.checkpoint_path)

    def _save_checkpoint(
        self,
        *,
        started_at: datetime,
        completed_rounds: list[dict[str, Any]],
        current_round: int,
        total_candidates: int,
        current_round_scores: dict[str, float],
        best_metric: float,
    ) -> None:
        assert self.checkpoint_path is not None
        direction = "minimize" if is_minimize(self.config.metric) else "maximize"
        cp = SelectionCheckpoint(
            run_name=self.run_name,
            started_at=started_at,
            updated_at=datetime.now(),
            completed_rounds=completed_rounds,
            current_round=current_round,
            total_candidates=total_candidates,
            current_round_scores=current_round_scores,
            best_metric=best_metric,
            direction=direction,
            max_features=self.config.features.max_features or 0,
        )
        save_checkpoint(self.checkpoint_path, cp)

    def _improvement(self, current: float, new: float) -> float:
        """Positive = better. For lower-is-better metrics, flip sign."""
        if is_minimize(self.config.metric):
            return current - new
        # roc_auc: higher is better
        return new - current

    def _is_better(self, a: float, b: float) -> bool:
        """True if score `a` is strictly better than `b` under the configured metric.

        Non-finite `a` is never better; non-finite `b` is always worse than a
        finite `a`. This ensures round 1 with no baseline (current_score=±inf)
        picks by raw score rather than tying all candidates at delta=inf.
        """
        if not math.isfinite(a):
            return False
        if not math.isfinite(b):
            return True
        if is_minimize(self.config.metric):
            return a < b
        return a > b

    def _build_candidate_model(
        self,
        match_level: list[str],
        point_level: list[str],
        scoring_params: dict,
    ) -> "ScoreStateChainServeModel | Any":
        """The estimator a candidate feature set is scored through.

        Single-level (`serve_component is None`) keeps the existing behaviour
        exactly: a `ScoreStateChainServeModel` over the candidate lists.

        Two-level routes through `build_serve_model`, with ONLY the named
        component's lists replaced by the candidate set and the other two held
        at their configured values. Going through the factory rather than
        constructing here is deliberate — this call site previously hardcoded
        the single-level class, which meant a `serve_model.type` of anything
        else was silently ignored and FS selected features for a model the user
        was not going to run.
        """
        from mvp.projection.iid.serve_model import build_serve_model

        component = self.config.serve_component
        if component is None:
            return ScoreStateChainServeModel(
                model_type=self.config.scoring_model.type,
                match_level_features=list(match_level),
                point_level_features=list(point_level),
                params=scoring_params,
                points_path=self.points_path,
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
                engine=self._engine,
            )

        base = self.config.serve_model
        if base is None:
            raise ValueError(
                "serve_component is set but serve_model is missing — the "
                "non-selected components have no feature sets to hold fixed, "
                "and defaulting them to empty would score the named component "
                "against a model unlike the one being built"
            )
        cfg = base.model_copy(deep=True)
        cfg.type = "two_level"
        cfg.model_type = self.config.scoring_model.type
        cfg.params = dict(scoring_params)
        if component == "first_in":
            # first_in is fit at (match, server) grain with no ScoreState, so a
            # STATE-DERIVABLE point feature has nothing to be evaluated at and
            # would be silently ignored — refuse instead.
            #
            # MATCH-CONSTANT point features are fine and are not refused: the
            # surface one-hots live only in the point pool (there is no
            # registered match-level surface indicator), they do not vary by
            # state, and first-serve rate plausibly varies by surface. Blocking
            # the whole list would starve the component of its only route to
            # them, which is the same silent-wrong the refusal exists to stop.
            #
            # The boundary is `ScoreStateChainServeModel._STATE_DERIVABLE`
            # itself, not a copy — the narrowing is only safe if it is exactly
            # the set the win branches already route on, so it must not be
            # restated here where the two could drift.
            bad = sorted(
                set(point_level) & ScoreStateChainServeModel._STATE_DERIVABLE
            )
            if bad:
                raise ValueError(
                    f"serve_component=first_in cannot take state-derivable "
                    f"point candidates (got {bad}); it is fit at match grain "
                    "with no ScoreState. Match-constant point features (the "
                    "surface one-hots) are accepted."
                )
            cfg.first_in_match_features = list(match_level)
            cfg.first_in_point_features = list(point_level)
        elif component == "win_first":
            cfg.win_first_match_features = list(match_level)
            cfg.win_first_point_features = list(point_level)
        elif component == "win_second":
            cfg.win_second_match_features = list(match_level)
            cfg.win_second_point_features = list(point_level)
        else:  # pragma: no cover - Literal-constrained
            raise ValueError(f"unknown serve_component: {component!r}")
        return build_serve_model(cfg, engine=self._engine)

    @staticmethod
    def _stop_record(
        round_idx: int,
        reason: str,
        current_score: float,
        best_cand: str | None,
        best_score: float,
        this_round_scores: dict[str, float],
        minimize: bool,
    ) -> dict[str, Any]:
        """History record for a halted round: the classification path's `stop`
        shape (selection.py) plus the finite-ranked candidates of the round."""
        ranked = sorted(
            ((f, m) for f, m in this_round_scores.items() if math.isfinite(m)),
            key=lambda x: x[1],
            reverse=not minimize,
        )
        return {
            "round": round_idx,
            "action": "stop",
            "reason": reason,
            "metric": current_score,
            "best_candidate": best_cand,
            "best_candidate_metric": best_score if best_cand is not None else None,
            "n_non_finite": len(this_round_scores) - len(ranked),
            "ranking": ranked,
        }

    def _score_cv(
        self,
        df: pl.DataFrame,
        splits: list[tuple[list[int], list[int]]],
        match_level: list[str],
        point_level: list[str],
    ) -> float:
        if needs_match_grain_prep(self.config.metric):
            return self._score_cv_match_grain(match_level, point_level)
        feature_cols = self._resolve_cols(match_level, point_level)
        fold_scores: list[float] = []
        for train_idx, test_idx in splits:
            train_df = df[train_idx]
            test_df = df[test_idx]
            X_train = train_df.select(feature_cols).to_numpy()
            y_train = train_df["point_won_by_server"].cast(pl.Int64).to_numpy()
            X_test = test_df.select(feature_cols).to_numpy()
            y_test = test_df["point_won_by_server"].cast(pl.Int64).to_numpy()

            model = build_score_state_model(
                type_=self.config.scoring_model.type,
                feature_names=feature_cols,
                params=dict(self.config.scoring_model.params),
            )
            model.fit(X_train, y_train)
            y_prob = model.predict_proba(X_test)
            metrics = compute_metrics(y_test, y_prob)
            fold_scores.append(metrics[self.config.metric])
        return float(np.mean(fold_scores))

    def _prepare_match_data(
        self,
        *,
        match_pool: list[str],
        engine: FeatureEngine,
        cache_key: str,
    ) -> None:
        """Build match-grain df + splits for chain-metric scoring. Idempotent."""
        if self._match_df is not None:
            return

        # Columns needed for target resolution, filtering, chain eval.
        # `surface` is required so the chain serve model can materialize
        # surface one-hots (`is_surface_hard`/etc.) when those are candidates.
        cols = [
            "match_uid", "player_id", "opp_id", "best_of", "won",
            "effective_match_date", "reason", "surface",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
        ]
        for c in self.config.data.filters:
            if c not in cols:
                cols.append(c)
        available = set(pl.scan_parquet(self.matches_path).collect_schema().names())
        cols = [c for c in cols if c in available]
        df = pl.read_parquet(self.matches_path, columns=cols)

        dr = self.config.data.date_range
        if dr is not None:
            if dr.start is not None:
                df = df.filter(pl.col("effective_match_date") >= dr.start)
            if dr.end is not None:
                df = df.filter(pl.col("effective_match_date") <= dr.end)

        # Load any computed-feature filter columns onto df before apply_filters
        # (e.g. `player_svc_elo_matchup: {abs_min: X}`). apply_filters references
        # the raw filter key, so the column must exist on df at filter time.
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        if filter_specs:
            df = engine.load_features_numpy(filter_specs, df, cache_key)

        df = apply_filters(df, self.config.data.filters)

        # Mirror IIDProjectionRunner._resolve_targets / _collapse_to_match_rows.
        df = df.filter(
            pl.col("player_set1_games").is_not_null()
            & pl.col("player_set2_games").is_not_null()
        )
        if "reason" in df.columns:
            df = df.filter(
                pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
            )
        df = df.with_columns(
            total_games_won().cast(pl.Float64).alias("_target_games_a"),
            total_games_lost().cast(pl.Float64).alias("_target_games_b"),
        )
        df = df.filter(
            pl.col("_target_games_a").is_not_null()
            & pl.col("_target_games_b").is_not_null()
            & pl.col("best_of").is_in([3, 5])
        )
        # Capture both player perspectives before deduplication — needed to build
        # the two-sided match feature frame used to avoid engine.compute() per fit call.
        both_sides_keys = df.select(["match_uid", "player_id", "opp_id"])

        df = df.sort(["match_uid", "player_id"]).unique(
            subset=["match_uid"], keep="first", maintain_order=True,
        )

        # Materialize every candidate match-level feature ON THE TWO-SIDED frame,
        # then join onto the match-grain (one-row-per-match) df.
        #
        # ScoreStateChainServeModel.predict_state_fn reads BOTH sides of every
        # mirror feature — `opp_X` for a player_ spec at the swap side, and
        # `player_X` for an opp_ spec (mirror=False diffs negate `player_X`
        # instead) — so the partner specs from swap_side_partner_specs have to
        # be requested here, not just the configured pool. The narrower
        # swap_side_opp_specs only covered player_ specs, which crashed the
        # first pool carrying opp_-only TRANSFORM candidates
        # (opp_vs_opp_style_pool_elo: registered mirror features leave their
        # player_ column behind as a load byproduct, transform outputs do
        # not). Two-sided is load-bearing: load_features_numpy derives an
        # opp_ column by self-joining the partner row on (match_uid, opp_id),
        # so on an already-deduplicated frame every opp_ column comes back
        # all-null.
        #
        # The two-sided frame is retained: the FS loop hands it to model.fit() per
        # candidate × fold, which is what keeps fit off engine.compute() (a full
        # 1.67M-row matches.parquet read each time).
        fixed_specs = self._fixed_arm_match_specs()
        load_specs = list(match_pool)
        seen = set(load_specs)
        n_fixed_extra = 0
        for spec in fixed_specs:
            if spec not in seen:
                seen.add(spec)
                load_specs.append(spec)
                n_fixed_extra += 1
        # Partners are taken over the UNION, not over match_pool: win_second's
        # fixed matchup specs are mirror=True, so their swap side reads opp_
        # columns that no pool member would have pulled in.
        n_swap_extra = 0
        for spec in swap_side_partner_specs(load_specs):
            if spec not in seen:
                seen.add(spec)
                load_specs.append(spec)
                n_swap_extra += 1
        self._match_features_both_sides = engine.load_features_numpy(
            load_specs, both_sides_keys, cache_key,
        )

        # Join the server-perspective row's features (plus its opp_ columns) on
        # (match_uid, player_id). Columns df already carries — computed filter
        # specs loaded above — are skipped so the join can't produce `_right`
        # duplicates; their values are identical either way.
        already_present = set(df.columns) - {"match_uid", "player_id"}
        feature_cols = [
            c for c in self._match_features_both_sides.columns
            if c not in already_present and c not in ("match_uid", "player_id", "opp_id")
        ]
        df = df.join(
            self._match_features_both_sides.select(
                ["match_uid", "player_id", *feature_cols]
            ),
            on=["match_uid", "player_id"],
            how="left",
        )

        val = self.config.validation
        splitter = make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
            initial_train_size=val.initial_train_size,
            step_size=val.step_size,
            train_size=val.train_size,
            test_start=getattr(val, "test_start", None),
            train_months=getattr(val, "train_months", None),
            initial_train_months=getattr(val, "initial_train_months", None),
            test_months=getattr(val, "test_months", None),
        )
        splits = list(splitter.split(df))

        self._match_splits = splits
        self._fs_match_splits = self._maybe_subsample_match_splits(splits)
        logger.info(
            "Chain-metric path: match-grain df=%d matches, %d folds, %d candidate match feats "
            "materialized (+%d fixed-arm, +%d swap-side cols)",
            len(df), len(splits), len(match_pool), n_fixed_extra, n_swap_extra,
        )

        # Pre-load points filtered to training matches to avoid 7.1M-row parquet read per fit call.
        match_uids = df["match_uid"].to_list()
        self._preloaded_points = pl.read_parquet(self.points_path).filter(
            pl.col("match_uid").is_in(match_uids)
        )
        logger.info(
            "Chain-metric path: pre-loaded %d two-sided match feature rows, %d point rows",
            len(self._match_features_both_sides), len(self._preloaded_points),
        )
        # Assigned LAST, because it is the idempotence key: the guard at the top
        # of this method early-returns on `_match_df is not None`. Setting it
        # before the points read would let a retry after a failed read return
        # having skipped _build_chain_folds, leaving _chain_folds None.
        self._match_df = df
        self._build_chain_folds()

    def _build_chain_folds(self) -> None:
        """Materialize per-fold fit/score inputs once. See `_ChainFold`.

        Residency is traded for churn deliberately: the fold frames are held for
        the whole run instead of being rebuilt per candidate. The sizes are logged
        because the trade is only favourable while they fit — a wide candidate
        pool (empty `candidate_match_level_features` → the full registered pool)
        makes `_match_features_both_sides` wide, and every fold slice inherits that
        width. `fs_match_subsample` is what bounds it.

        Deliberately no `check_memory` here. It reads SYSTEM-WIDE load, not this
        process's, so calling it on a path that unit tests exercise
        (`test_serve_discovery_swap_side.TestPrepareMatchData`) makes those tests
        fail whenever the dev box happens to sit above the limit — a real failure
        seen when this was first written. The per-fold guard in `_score_cv_chain`
        already bounds the loop and reaches the same condition one candidate
        later; the logged sizes below are what make the build itself auditable.
        """
        assert self._match_df is not None and self._fs_match_splits is not None, (
            "_build_chain_folds called before the match frame was prepared"
        )
        assert (
            self._match_features_both_sides is not None
            and self._preloaded_points is not None
        ), "_build_chain_folds called before the preloaded frames were built"
        # An empty split list is silent-wrong downstream, not loud: every
        # candidate would score `np.mean([])` = nan, `_is_better` would reject all
        # of them, and the run would end with zero selected features and no error.
        if not self._fs_match_splits:
            raise ValueError(
                "chain-metric FS has no folds — the validation config produced no "
                "splits over the filtered match frame. Widen data.date_range or "
                "lower validation.initial_train_months/test_months."
            )
        t0 = time.perf_counter()
        branch_run = is_branch_metric(self.config.metric)
        folds: list[_ChainFold] = []
        for train_idx, test_idx in self._fs_match_splits:
            train_df = self._match_df[train_idx]
            train_uids = set(train_df["match_uid"].to_list())
            test_df = self._match_df[test_idx]
            test_points = test_first_in = None
            if branch_run:
                test_uids = set(test_df["match_uid"].to_list())
                held = self._preloaded_points.filter(
                    pl.col("match_uid").is_in(test_uids)
                )
                if self.config.serve_component == FIRST_IN:
                    test_first_in = first_in_counts(held)
                else:
                    test_points = held.select(
                        [c for c in _branch_point_columns() if c in held.columns]
                    )
            folds.append(
                _ChainFold(
                    train_df=train_df,
                    test_df=test_df,
                    feats=self._match_features_both_sides.filter(
                        pl.col("match_uid").is_in(train_uids)
                    ),
                    points=self._preloaded_points.filter(
                        pl.col("match_uid").is_in(train_uids)
                    ),
                    test_points=test_points,
                    test_first_in=test_first_in,
                )
            )
        self._chain_folds = folds
        # Sizes are GiB, matching check_memory's units (engine.py) so the two read
        # against each other in the log. Parents are reported separately rather
        # than folded in: _match_df and _match_features_both_sides stay live for
        # the run, so the fold total alone is not this path's residency.
        gib = 1024 ** 3
        fold_bytes = sum(
            f.train_df.estimated_size() + f.test_df.estimated_size()
            + f.feats.estimated_size() + f.points.estimated_size()
            + (f.test_points.estimated_size() if f.test_points is not None else 0)
            + (f.test_first_in.estimated_size() if f.test_first_in is not None else 0)
            for f in folds
        )
        parent_bytes = (
            self._match_df.estimated_size()
            + self._match_features_both_sides.estimated_size()
        )
        # Nothing reads _preloaded_points after this point — the per-fold slices
        # above are its only consumers — so drop it rather than carry the full
        # unfiltered frame for the rest of the run. _match_features_both_sides is
        # NOT droppable: test_serve_discovery_swap_side.py reads it post-build.
        self._preloaded_points = None
        held_bytes = sum(
            (f.test_points.estimated_size() if f.test_points is not None else 0)
            + (f.test_first_in.estimated_size() if f.test_first_in is not None else 0)
            for f in folds
        )
        logger.info(
            "Match-grain path: cached %d fold input sets in %.1fs "
            "(%.2f GiB folds + %.2f GiB retained parents = %.2f GiB); "
            "train matches %s%s",
            len(folds), time.perf_counter() - t0,
            fold_bytes / gib, parent_bytes / gib,
            (fold_bytes + parent_bytes) / gib,
            [len(f.train_df) for f in folds],
            # Test rows are NOT subsampled (fs_match_subsample caps train
            # only), so this is the one piece that scales with the full test
            # fold; logged separately because it is the branch route's cost.
            f"; held-out branch inputs {held_bytes / gib:.2f} GiB" if held_bytes else "",
        )
        # Up front, before any candidate thread exists — never lazily from a
        # worker (the folds above are shared across threads by design).
        self._build_prefit_fixed()

    def _scoring_params(self) -> dict[str, Any]:
        """The scorer params every CHAIN-PATH candidate model is built with
        (the point-grain `_score_cv` path builds its own, uninjected).

        ONE derivation site: `_build_prefit_fixed` must fit the fixed
        two-level components with exactly what the candidate path uses, or
        the model fitted once per fold is not the model the candidate loop
        would have fitted.
        """
        params = dict(self.config.scoring_model.params)
        # Per-fit threads are the config's to set. Logistic ignores n_jobs
        # (sklearn >=1.8 no-op, and injecting it spams FutureWarnings) → never
        # inject for it. Xgboost honors n_jobs as OpenMP threads → default to 1
        # when unset so concurrent candidate threads don't oversubscribe; the
        # validator caps n_parallel_candidates * n_jobs at the cpu count.
        if self.config.scoring_model.type != "logistic" and "n_jobs" not in params:
            params["n_jobs"] = 1
        return params

    def _build_prefit_fixed(self) -> None:
        """Fit the two FIXED components of a two-level component run once per
        fold, so the candidate loop only fits the component under selection.

        `TwoLevelServeModel.fit` fits all three components; in a component
        run the other two are held at their configured sets, so per-candidate
        they were refit on identical rows with identical params and seed —
        the identical model, ~460 times a round. Built here, before any
        candidate thread exists. The selected component is passed EMPTY so
        its placeholder branch costs nothing and is discarded; only the two
        fixed components are kept, and each candidate gets a private
        deepcopy (`_attach_prefit`), so nothing is shared across threads.

        Equivalence with the per-candidate refit is the contract: same fold
        frames, same `_scoring_params()`, same deterministic fit.
        """
        component = self.config.serve_component
        self._prefit_fixed = None
        if component is None or self._chain_folds is None:
            return
        t0 = time.perf_counter()
        params = self._scoring_params()
        # Same BLAS cap the candidate loop fits under: a no-op for xgboost
        # (OpenMP), but a logistic scorer's lbfgs reduction order depends on
        # the thread count, and "fitted once per fold" must be the same fit.
        _, cap_n_jobs = resolve_candidate_parallelism(
            params.get("n_jobs"), self.config.n_parallel_candidates,
        )
        prefit: dict[int, dict[str, Any]] = {}
        with blas_thread_cap(self.config.scoring_model.type, cap_n_jobs):
            for fold_idx, fold in enumerate(self._chain_folds):
                model = self._build_candidate_model([], [], params)
                if not isinstance(model, TwoLevelServeModel):
                    raise TypeError(
                        "serve_component is set but _build_candidate_model "
                        f"returned {type(model).__name__}, not TwoLevelServeModel"
                    )
                model.fit(
                    fold.train_df,
                    preloaded_match_features=fold.feats,
                    preloaded_points=fold.points,
                )
                prefit[fold_idx] = {
                    name: fitted for name, fitted in model.components().items()
                    if name != component
                }
        self._prefit_fixed = prefit
        logger.info(
            "Prefit fixed two-level components %s once per fold "
            "(%d folds, %.1fs); candidates fit only %s",
            sorted(next(iter(prefit.values()))), len(prefit),
            time.perf_counter() - t0, component,
        )

    def _attach_prefit(self, model: Any, fold_idx: int) -> None:
        """Hand a candidate model private copies of the fold's prefit
        components. No-op for single-level runs."""
        if self._prefit_fixed is None or not isinstance(model, TwoLevelServeModel):
            return
        for component, fitted in self._prefit_fixed[fold_idx].items():
            model.attach_prefit(component, copy.deepcopy(fitted))

    _PHASE_KEYS = ("load", "join", "derive", "matrix", "fit", "predict", "dp", "score")

    def _record_phases(self, timing: dict[str, float], n_folds: int) -> None:
        with self._phase_lock:
            for k, v in timing.items():
                self._phase_totals[k] = self._phase_totals.get(k, 0.0) + v
            self._phase_cand_folds += n_folds

    def _take_phase_means(self) -> dict[str, float]:
        """Mean seconds per candidate-fold by phase since the last call; resets."""
        with self._phase_lock:
            n = self._phase_cand_folds
            means = {k: v / n for k, v in self._phase_totals.items()} if n else {}
            self._phase_totals = {}
            self._phase_cand_folds = 0
        return means

    @classmethod
    def _format_phases(cls, means: dict[str, float]) -> str:
        """Only ever printed from the [diag] line, i.e. only when a
        checkpoint path and interval are set — a checkpoint-less run emits no
        phase readout."""
        if not means:
            return "phases=n/a"
        parts = [f"{k}={means[k]:.2f}" for k in cls._PHASE_KEYS if k in means]
        return "phases(s/cand-fold): " + " ".join(parts)

    def _score_cv_match_grain(
        self, match_level: list[str], point_level: list[str],
    ) -> float:
        """Score one candidate set on the match-grain folds.

        Two routes share those folds: the composed chain, and a per-branch
        metric scoring the selected component on its own target. One entry
        point so the serial and the parallel candidate loops cannot dispatch
        differently — routing only one of them is how a branch run would end
        up scored through the chain.
        """
        if is_branch_metric(self.config.metric):
            return self._score_cv_branch(match_level, point_level)
        return self._score_cv_chain(match_level, point_level)

    def _score_cv_branch(
        self, match_level: list[str], point_level: list[str],
    ) -> float:
        """Score the SELECTED component on its own held-out target.

        No composition, no chain, no DP: the candidate model is built and
        fitted exactly as the chain route builds it (same
        `_build_candidate_model` / `_scoring_params`, same prefit for the two
        fixed components), then only the component under selection is asked
        for predictions.
        """
        assert self._chain_folds is not None, (
            "_score_cv_branch called before _prepare_match_data"
        )
        if not match_level and not point_level:
            return worst_score(self.config.metric)
        component = self.config.serve_component
        base = base_metric_of(self.config.metric)
        fold_scores: list[float] = []
        timing: dict[str, float] = {}

        for fold_idx, fold in enumerate(self._chain_folds):
            model = self._build_candidate_model(
                match_level, point_level, self._scoring_params(),
            )
            self._attach_prefit(model, fold_idx)
            model.fit(
                fold.train_df,
                preloaded_match_features=fold.feats,
                preloaded_points=fold.points,
            )
            for k, v in getattr(model, "fit_timings", {}).items():
                timing[k] = timing.get(k, 0.0) + v
            branch = model.components()[component]
            t = time.perf_counter()
            if component == FIRST_IN:
                score = self._score_first_in(model, fold)
            else:
                score = self._score_win_branch(branch, fold, base)
            timing["score"] = timing.get("score", 0.0) + (time.perf_counter() - t)
            fold_scores.append(score)
            del model, branch
            check_memory("serve FS branch scoring")

        self._record_phases(timing, len(self._chain_folds))
        gc.collect()
        return float(np.mean(fold_scores))

    def _score_win_branch(self, branch: Any, fold: _ChainFold, base: str) -> float:
        """A win branch on `point_won_by_server` over its own serve rows."""
        if isinstance(branch, _ConstantBranch):
            raise TypeError(
                "branch scoring needs a fitted branch, got _ConstantBranch — "
                "the component under selection must carry features"
            )
        assert fold.test_points is not None, (
            "branch run without a held-out point frame; _build_chain_folds "
            "materializes it only when the metric is per-branch"
        )
        joined = branch.build_test_point_frame(
            fold.test_df,
            preloaded_match_features=self._match_features_both_sides,
            preloaded_points=fold.test_points,
        )
        if joined is None or len(joined) == 0:
            return worst_score(self.config.metric)
        return branch.score_test_frame(joined)[base]

    def _score_first_in(self, model: Any, fold: _ChainFold) -> float:
        """first_in: weighted MSE on the held-out (match, server) rate.

        The A/B orientation is the hazard here — `test_df` is one row per
        match carrying `player_id` and `opp_id`, and joining the aggregate on
        the wrong one scores A's prediction against B's realised rate and
        returns a plausible number.

        Stacking both sides into one keyed frame and joining with
        `validate="1:1"` catches the SHAPE error: the aggregate has two rows
        per match, so joining it at match grain is 1:m and cannot pass. It
        does NOT catch a swapped A/B stack, which is still one-to-one —
        `test_perfect_prediction_scores_zero_and_the_swap_does_not` is what
        pins the orientation, and it is the test to keep if this is ever
        refactored.
        """
        assert fold.test_first_in is not None, (
            "first_in branch run without a held-out aggregate"
        )
        fi_a, fi_b = model._first_in_for(fold.test_df)
        pred = pl.concat([
            fold.test_df.select(
                "match_uid",
                pl.col("player_id").alias("server_id"),
            ).with_columns(pl.Series("_pred", fi_a)),
            fold.test_df.select(
                "match_uid",
                pl.col("opp_id").alias("server_id"),
            ).with_columns(pl.Series("_pred", fi_b)),
        ])
        joined = pred.join(
            fold.test_first_in, on=["match_uid", "server_id"],
            how="inner", validate="1:1",
        )
        if joined.height == 0:
            return worst_score(self.config.metric)
        if joined["_n_serve_pts"].null_count() or joined["_pred"].null_count():
            raise ValueError("first_in branch scoring: null predictions or weights")
        w = joined["_n_serve_pts"].to_numpy().astype(np.float64)
        rate = (joined["_n_first_in"] / joined["_n_serve_pts"]).to_numpy()
        pred_v = joined["_pred"].to_numpy().astype(np.float64)
        return float(np.sum(w * (pred_v - rate) ** 2) / np.sum(w))

    def _score_cv_chain(
        self, match_level: list[str], point_level: list[str],
    ) -> float:
        assert self._chain_folds is not None, (
            "_score_cv_chain called before _prepare_match_data"
        )
        if not match_level and not point_level:
            return worst_score(self.config.metric)
        fold_scores: list[float] = []
        timing: dict[str, float] = {}

        def _acc(key: str, secs: float) -> None:
            timing[key] = timing.get(key, 0.0) + secs

        for fold_idx, fold in enumerate(self._chain_folds):
            test_df = fold.test_df

            model = self._build_candidate_model(
                match_level, point_level, self._scoring_params(),
            )
            self._attach_prefit(model, fold_idx)
            model.fit(
                fold.train_df,
                preloaded_match_features=fold.feats,
                preloaded_points=fold.points,
            )
            for k, v in getattr(model, "fit_timings", {}).items():
                _acc(k, v)
            # Phase buckets, read literally when gating Phase 2:
            #   predict = building the state fns + `predict` — which is
            #             predict_state_fn at the neutral state, so this bucket
            #             holds a SECOND, redundant matrix build per fold
            #             (serve_model.py `predict`). Removing it is
            #             equivalence-by-construction but outside the approved
            #             plan; it needs its own test before it moves.
            #   dp      = the set/match DP INCLUDING every state-conditioned
            #             predict_proba the DP triggers through p_a_fn/p_b_fn
            #             (cached per state key inside the model).
            t = time.perf_counter()
            p_a_fn, p_b_fn = model.predict_state_fn(test_df)
            p_a, p_b = model.predict(test_df)
            _acc("predict", time.perf_counter() - t)
            best_of = test_df["best_of"].to_numpy().astype(np.int64)
            t = time.perf_counter()
            dist = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)
            _acc("dp", time.perf_counter() - t)

            y_games_a = test_df["_target_games_a"].to_numpy().astype(np.float64)
            y_games_b = test_df["_target_games_b"].to_numpy().astype(np.float64)
            t = time.perf_counter()
            fold_scores.append(
                score_chain(
                    self.config.metric, dist, y_games_a, y_games_b,
                    total_lines=list(self.config.metrics.total_lines),
                    spread_lines=list(self.config.metrics.spread_lines),
                    y_won=test_df["won"].to_numpy().astype(np.int64),
                )
            )
            _acc("score", time.perf_counter() - t)
            # Drop refs promptly, and bound memory with the same check_memory()
            # guard the rest of the codebase uses (classification FS at
            # fast_selection.py:522, the projection runners per fold). Aborts
            # cleanly if over --memory-limit.
            #
            # Only the per-candidate objects are dropped. train_df / test_df /
            # preloaded_* are borrowed from self._chain_folds and live for the
            # whole run by design, so deleting those names would free nothing and
            # would misrepresent what is reclaimable here.
            del model, p_a_fn, p_b_fn, p_a, p_b, dist
            check_memory("serve FS chain scoring")

        self._record_phases(timing, len(self._chain_folds))

        # Kept on evidence, not on the explanation that used to sit here.
        # Measured before this call was added: RSS climbed ~22 MB per
        # candidate (10789 -> 12449 MB over 75 candidates) and s/it tracked it
        # almost exactly, 7.98 -> 11.89; it never plateaued, and at that rate
        # a run trips --memory-limit partway through round 1. The earlier
        # comment attributed that to a model<->closure reference cycle through
        # the arrays predict_state_fn assigned onto the instance. That was not
        # a cycle (closures -> self -> arrays has no edge back to the closures;
        # refcounting alone reclaims it), and those arrays are closure locals
        # now (serve_model.py `_build_state_fns`). Whatever the collect is
        # actually reclaiming has not been identified — plan
        # 2026-09-03-serve-fs-wall-time, Phase 1 item 7: diagnose
        # (DEBUG_SAVEALL, then tracemalloc / gc.get_objects deltas), and drop
        # this only if the [diag] rss stays flat over >=100 candidates.
        #
        # Once per CANDIDATE, not per fold. An earlier comment here rejected
        # gc.collect() because a per-fold call under n_parallel_candidates > 1
        # would stall every worker on the GIL — true, but this is a quarter of
        # that frequency against a ~10s candidate. Same pattern the tuner uses
        # per Optuna trial (tuning.py:989-991).
        gc.collect()
        return float(np.mean(fold_scores))

    def _resolve_cols(self, match_level: list[str], point_level: list[str]) -> list[str]:
        cols: list[str] = []
        for spec in match_level:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col = build_column_name(full_name, params)
            if col.startswith("player_"):
                col = "server_" + col[len("player_"):]
            elif col.startswith("opp_"):
                col = "returner_" + col[len("opp_"):]
            cols.append(col)
        cols.extend(point_level)
        return cols

    def _maybe_subsample_splits(
        self,
        splits: list[tuple[list[int], list[int]]],
    ) -> list[tuple[list[int], list[int]]]:
        """Subsample train indices per fold for fast candidate scoring.

        Test indices are kept at full size so held-out metric values stay
        comparable across candidates. Sampled indices are sorted to preserve
        the walk-forward time order within train.
        """
        cap = self.config.fs_train_subsample
        if cap is None:
            return splits
        rng = np.random.default_rng(self.config.fs_subsample_seed)
        sampled: list[tuple[list[int], list[int]]] = []
        for train_idx, test_idx in splits:
            if len(train_idx) > cap:
                idx_arr = np.asarray(train_idx)
                picked = rng.choice(idx_arr, size=cap, replace=False)
                picked.sort()
                sampled.append((picked.tolist(), test_idx))
            else:
                sampled.append((train_idx, test_idx))
        original_sizes = [len(t) for t, _ in splits]
        sampled_sizes = [len(t) for t, _ in sampled]
        logger.info(
            "FS train subsample: cap=%d, train sizes %s → %s",
            cap, original_sizes, sampled_sizes,
        )
        return sampled

    def _maybe_subsample_match_splits(
        self,
        splits: list[tuple[list[int], list[int]]],
    ) -> list[tuple[list[int], list[int]]]:
        """Subsample train match indices per fold for fast chain-metric candidate scoring.

        Mirrors _maybe_subsample_splits but operates on match-grain splits used by
        _score_cv_chain. Test indices are kept at full size. Final-form eval always
        uses the full _match_splits so reported metrics are honest.
        """
        cap = self.config.fs_match_subsample
        if cap is None:
            return splits
        rng = np.random.default_rng(self.config.fs_subsample_seed)
        sampled: list[tuple[list[int], list[int]]] = []
        for train_idx, test_idx in splits:
            if len(train_idx) > cap:
                idx_arr = np.asarray(train_idx)
                picked = rng.choice(idx_arr, size=cap, replace=False)
                picked.sort()
                sampled.append((picked.tolist(), test_idx))
            else:
                sampled.append((train_idx, test_idx))
        original_sizes = [len(t) for t, _ in splits]
        sampled_sizes = [len(t) for t, _ in sampled]
        logger.info(
            "Chain FS match subsample: cap=%d, train sizes %s → %s",
            cap, original_sizes, sampled_sizes,
        )
        return sampled

    def _make_splitter(self) -> Any:
        val = self.config.point_validation
        return make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
            initial_train_size=val.initial_train_size,
            step_size=val.step_size,
            train_size=val.train_size,
            test_start=getattr(val, "test_start", None),
            train_months=getattr(val, "train_months", None),
            initial_train_months=getattr(val, "initial_train_months", None),
            test_months=getattr(val, "test_months", None),
        )

    def _fixed_arm_match_specs(self) -> list[str]:
        """Match specs the fits READ but FS never SEARCHES.

        `match_pool` is the candidate list; it is not what gets fitted. A
        two_level component run holds the other two components fixed at sets
        that come from `serve_model`, and those specs were only ever
        materialized because the pool happened to be a superset of them.
        Trimming first_in's pool to server-own specs broke that coincidence and
        every fixed win_first diff went missing at the first prefit.

        Both `_pre_cache_all` and `_prepare_match_data` read this. They have to
        agree: `ensure_cached` deletes every cached parquet when the data key
        moves (monthly, or on --refresh) and recomputes only what it was
        handed, so a spec loaded here but not cached there is a file that no
        longer exists. Unioning into the base list is also what retires the old
        base-must-be-in-the-candidate-pool guard.

        `match_level_features` is deliberately absent: no FS path builds a
        model that reads it. `match_level_columns` likewise, and it would
        actually break — it holds resolved column names, not specs.
        """
        specs: list[str] = list(self.config.features.base_match_level_features)
        serve_model = self.config.serve_model
        if serve_model is not None:
            for component in (
                serve_model.first_in_match_features,
                serve_model.win_first_match_features,
                serve_model.win_second_match_features,
            ):
                for spec in component:
                    if spec not in specs:
                        specs.append(spec)
        return specs

    def _pre_cache_all(
        self,
        *,
        base_match: list[str],
        candidate_match: list[str],
    ) -> tuple[FeatureEngine, str]:
        """Cache all match-level specs to disk without loading them into memory."""
        all_match_specs: list[str] = []
        for spec in base_match + candidate_match + self._fixed_arm_match_specs():
            if spec not in all_match_specs:
                all_match_specs.append(spec)
        for spec in get_filter_feature_specs(self.config.data.filters):
            if spec not in all_match_specs:
                all_match_specs.append(spec)

        extra_columns = ["circuit", "surface", "round", "best_of"]
        for col in self.config.data.filters:
            if col not in extra_columns:
                extra_columns.append(col)

        engine = make_fs_engine(matches_path=self.matches_path, cache_dir=self.cache_dir)
        cache_key = engine.ensure_cached(all_match_specs, extra_columns=extra_columns)
        return engine, cache_key

    def _build_base_matrix(
        self,
        engine: FeatureEngine,
        cache_key: str,
        *,
        base_match: list[str],
        base_point: list[str],
        candidate_point: list[str],
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Build the point-grain training matrix with only base match features loaded.

        Returns:
            (base_df, slim_matches) where slim_matches carries
            (match_uid, player_id, opp_id) for per-candidate lazy loading.
        """
        extra_columns = ["circuit", "surface", "round", "best_of"]
        for col in self.config.data.filters:
            if col not in extra_columns:
                extra_columns.append(col)

        structural_cols = ["match_uid", "player_id", "opp_id", "effective_match_date"] + extra_columns
        available = set(pl.scan_parquet(self.matches_path).collect_schema().names())
        structural_cols = [c for c in structural_cols if c in available]
        matches_df = pl.read_parquet(self.matches_path, columns=structural_cols)

        dr = self.config.data.date_range
        if dr is not None:
            if dr.start is not None:
                matches_df = matches_df.filter(pl.col("effective_match_date") >= dr.start)
            if dr.end is not None:
                matches_df = matches_df.filter(pl.col("effective_match_date") <= dr.end)

        # Retain a slim copy (with player_id / opp_id) for per-candidate loading.
        # Must be captured before the rename below.
        slim_cols = [c for c in ["match_uid", "player_id", "opp_id"] if c in matches_df.columns]
        slim_matches = matches_df.select(slim_cols)

        # Computed-feature filters (e.g. `player_svc_elo_matchup: {abs_min: X}`)
        # reference player_/opp_ column names. Load + apply them at match grain,
        # before the server_/returner_ rename and the points join. Remaining
        # (raw-column) filters are applied post-join below.
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        computed_filter_keys = set(filter_specs)
        if filter_specs:
            matches_df = engine.load_features_numpy(filter_specs, matches_df, cache_key)
            match_grain_filters = {
                k: v for k, v in self.config.data.filters.items()
                if k in computed_filter_keys
            }
            matches_df = apply_filters(matches_df, match_grain_filters)

        # Load only base match features from cache onto matches_df.
        matches_df = engine.load_features_numpy(base_match, matches_df, cache_key)

        # Rename player_*/opp_* → server_*/returner_* to align with point grain.
        renames = {"player_id": "server_id", "opp_id": "returner_id"}
        for col in matches_df.columns:
            if col.startswith("player_") and col != "player_id":
                renames[col] = "server_" + col[len("player_"):]
            elif col.startswith("opp_") and col != "opp_id":
                renames[col] = "returner_" + col[len("opp_"):]
        matches_df = matches_df.rename(renames)

        # Join match-level features to points.
        points = pl.read_parquet(self.points_path)
        logger.info("Loaded %d point rows", len(points))
        overlap = set(points.columns) & set(matches_df.columns) - {"match_uid", "server_id", "returner_id"}
        if overlap:
            matches_df = matches_df.drop(list(overlap))
        joined = points.join(matches_df, on=["match_uid", "server_id", "returner_id"], how="inner")

        # Add draw_type literal + all point-level features (base + candidates).
        if "draw_type" not in joined.columns:
            joined = joined.with_columns(pl.lit("singles").alias("draw_type"))
        all_point_names: list[str] = list(base_point)
        for f in candidate_point:
            if f not in all_point_names:
                all_point_names.append(f)
        joined = add_derived_point_features(joined, all_point_names)

        # Apply remaining (non-computed) domain filters at point grain.
        # Computed-feature filters were already applied at match grain above.
        point_grain_filters = {
            k: v for k, v in self.config.data.filters.items()
            if k not in computed_filter_keys
        }
        joined = apply_filters(joined, point_grain_filters)
        joined = joined.filter(pl.col("point_won_by_server").is_not_null())

        return joined, slim_matches

    def _extend_df_with_match_feature(
        self,
        df: pl.DataFrame,
        slim_matches: pl.DataFrame,
        engine: FeatureEngine,
        cache_key: str,
        spec: str,
    ) -> pl.DataFrame:
        """Load one match-level spec from cache and join its column(s) to df.

        slim_matches carries (match_uid, player_id[, opp_id]) so that
        load_features_numpy can join from cache. The result is renamed to the
        server_/returner_ convention used in the point-grain df, then joined on
        (match_uid, server_id, returner_id).
        """
        cand_df = engine.load_features_numpy([spec], slim_matches, cache_key)

        renames: dict[str, str] = {}
        for col in cand_df.columns:
            if col == "player_id":
                renames[col] = "server_id"
            elif col == "opp_id":
                renames[col] = "returner_id"
            elif col.startswith("player_"):
                renames[col] = "server_" + col[len("player_"):]
            elif col.startswith("opp_"):
                renames[col] = "returner_" + col[len("opp_"):]
        if renames:
            cand_df = cand_df.rename(renames)

        join_key_list = [k for k in ["match_uid", "server_id", "returner_id"] if k in cand_df.columns]
        extra_cols = [c for c in cand_df.columns if c not in set(join_key_list)]

        # Drop any column that already exists in df (shouldn't happen, but guard).
        to_drop = [c for c in extra_cols if c in df.columns]
        if to_drop:
            df = df.drop(to_drop)

        return df.join(
            cand_df.select(join_key_list + extra_cols),
            on=["match_uid", "server_id", "returner_id"],
            how="left",
        )
