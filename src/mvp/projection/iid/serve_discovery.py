"""Forward-selection discovery for the score-state serve model.

Caches all match-level candidate features to disk once, then iteratively adds
the candidate feature whose inclusion most improves the CV metric. Match-level
candidates are loaded lazily from cache one at a time — only the
currently-evaluated feature is joined to the base point-grain matrix, keeping
peak memory proportional to (rows × |selected| + |point_features|) rather than
(rows × |pool|). After FS terminates, optionally re-trains all configured
`model_forms` on the final feature set for comparison.
"""

import logging
import math
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
from mvp.model.engine import FeatureEngine, build_column_name, parse_feature_spec
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.metrics import compute_metrics
from mvp.model.splitters import make_splitter
from mvp.model.discovery.discover import get_all_feature_specs
from mvp.projection.iid.config import ServeDiscoveryConfig
from mvp.projection.iid.metrics import crps_discrete_pmf
from mvp.projection.iid.score_state_features import (
    add_derived_point_features,
    default_point_level_candidate_pool,
)
from mvp.projection.iid.score_state_model import build_score_state_model
from mvp.projection.iid.serve_model import ScoreStateChainServeModel
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn

logger = logging.getLogger(__name__)

_POINT_METRICS = {"log_loss", "brier_score", "roc_auc", "calibration_error"}
_CHAIN_METRICS = {"iid_crps_total_games", "iid_crps_spread", "mae", "rmse"}
# Point features that cannot be represented in the chain DP: the deuce
# closed-form in stateful_chain.hold_from_state_fn treats ("D","D") as a
# single absorbing node, so features that distinguish deuce iterations
# (point_num resets per game but advances through deuce cycles) would
# invalidate the closed form. Excluded from the chain-path candidate pool.
_CHAIN_INCOMPATIBLE_POINT_FEATURES = frozenset({"point_num"})
_MINIMIZE_METRICS = {
    "log_loss", "brier_score", "calibration_error",
    "iid_crps_total_games", "iid_crps_spread", "mae", "rmse",
}


def _score_dist_metric(
    metric: str,
    dist: Any,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
) -> float:
    """Score a MatchDistribution against observed games for a chain-grain metric."""
    if metric == "mae":
        return float(np.mean(np.abs(y_games_a - dist.expected_games_a)))
    if metric == "rmse":
        return float(np.sqrt(np.mean((y_games_a - dist.expected_games_a) ** 2)))
    if metric == "iid_crps_total_games":
        obs_total = (y_games_a + y_games_b).astype(np.int64)
        return crps_discrete_pmf(obs_total, dist.total_games_pmf)
    if metric == "iid_crps_spread":
        obs_spread = (y_games_a - y_games_b).astype(np.int64)
        obs_idx = obs_spread + dist.spread_offset
        obs_idx = np.clip(obs_idx, 0, dist.spread_pmf.shape[1] - 1)
        return crps_discrete_pmf(obs_idx, dist.spread_pmf)
    raise ValueError(f"Unknown chain metric: {metric}")


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
class FinalFormResult:
    form: str
    metrics: dict[str, float]
    coef_summary: dict[str, Any] | None


@dataclass
class DiscoveryResult:
    selected_match_level: list[str]
    selected_point_level: list[str]
    rounds: list[FSRoundResult]
    final_forms: list[FinalFormResult]
    n_train_rows: int


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
        checkpoint_interval: int = 50,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = ServeDiscoveryConfig.from_file(config_path)
        self.points_path = Path(points_path) if points_path else get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        self.matches_path = Path(matches_path) if matches_path else get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        self.cache_dir = Path(cache_dir) if cache_dir else get_local_data_root() / "features" / "cache"
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.run_name = run_name or self.config_path.stem
        self.checkpoint_interval = checkpoint_interval

        # Match-grain cache for chain-metric path (lazily populated).
        self._match_df: pl.DataFrame | None = None
        self._match_splits: list[tuple[list[int], list[int]]] | None = None
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
        if self.config.metric in _CHAIN_METRICS:
            dropped = [f for f in point_pool if f in _CHAIN_INCOMPATIBLE_POINT_FEATURES]
            if dropped:
                point_pool = [f for f in point_pool if f not in _CHAIN_INCOMPATIBLE_POINT_FEATURES]
                logger.info(
                    "Chain metric %s: excluding point features incompatible with deuce closed-form: %s",
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

        # Chain-metric path needs a match-grain df with all candidate match features
        # materialized, plus match-grain folds from config.validation.
        if self.config.metric in _CHAIN_METRICS:
            self._prepare_match_data(match_pool=match_pool, engine=engine, cache_key=cache_key)

        candidate_match = [c for c in match_pool if c not in selected_match]
        candidate_point = [c for c in point_pool if c not in selected_point]
        first_round_logged = False

        # Attempt to restore from checkpoint
        cp = self._load_checkpoint() if self.checkpoint_path else None
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
                    if self.config.metric not in _CHAIN_METRICS:
                        base_df = self._extend_df_with_match_feature(base_df, slim_matches, engine, cache_key, feat)
                else:
                    selected_point.append(feat)
                    if feat in candidate_point:
                        candidate_point.remove(feat)
                rounds.append(
                    FSRoundResult(
                        round_idx=len(rounds),
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
            partial_round_scores = dict(cp.current_round_scores)
            logger.info(
                "Resumed from checkpoint: %d completed rounds, current score=%.6f, partial scores for %d candidates",
                len(rounds), current_score, len(partial_round_scores),
            )
        else:
            if selected_match or selected_point:
                current_score = self._score_cv(base_df, fs_splits, selected_match, selected_point)
                logger.info("Base-only CV %s = %.6f (%d features)", self.config.metric, current_score, len(selected_match) + len(selected_point))
            else:
                current_score = float("inf") if self.config.metric in _MINIMIZE_METRICS else float("-inf")
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

            worst_score = float("inf") if self.config.metric in _MINIMIZE_METRICS else float("-inf")
            best_new_score = worst_score
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
                best_prev_cand = min(this_round_scores, key=this_round_scores.get) if self.config.metric in _MINIMIZE_METRICS else max(this_round_scores, key=this_round_scores.get)
                cand_score = this_round_scores[best_prev_cand]
                if self._is_better(cand_score, best_new_score):
                    best_new_score = cand_score
                    best_cand = best_prev_cand
                    best_grain = "match" if best_prev_cand in candidate_match else "point"
                logger.info(
                    "  Restored %d/%d candidate scores from checkpoint",
                    len(this_round_scores), total_cands,
                )

            bar = tqdm(tagged, desc=desc, leave=False, ncols=120)
            if best_cand is not None and hasattr(bar, "set_postfix"):
                bar.set_postfix(
                    best=f"{best_new_score:.6f}",
                    feat=f"{best_cand}[{best_grain}]",
                    refresh=False,
                )

            eval_count = 0
            chain_mode = self.config.metric in _CHAIN_METRICS
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
                logger.info("FS halting: no candidate exceeds min_delta=%.6f (best=%.6f)", self.config.min_delta, best_delta)
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

            if not first_round_logged and round_idx == 1:
                first_round_logged = True
                reverse = self.config.metric not in _MINIMIZE_METRICS
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

        # Final: train all configured forms on the selected feature set, report metrics.
        # base_df now contains all selected match features (added permanently during FS).
        final_forms: list[FinalFormResult] = []
        for form in self.config.model_forms:
            params = self.config.model_params.get(form, {})
            metrics, coefs = self._final_train_eval(base_df, splits, selected_match, selected_point, form, params)
            final_forms.append(FinalFormResult(form=form, metrics=metrics, coef_summary=coefs))
            logger.info("Final form %s: %s=%.6f", form, self.config.metric, metrics.get(self.config.metric, float("nan")))

        # Restore log levels silenced around the FS + final-form eval.
        for lg, lvl in prev_levels:
            lg.setLevel(lvl)

        return DiscoveryResult(
            selected_match_level=selected_match,
            selected_point_level=selected_point,
            rounds=rounds,
            final_forms=final_forms,
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
        direction = "minimize" if self.config.metric in _MINIMIZE_METRICS else "maximize"
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
        if self.config.metric in _MINIMIZE_METRICS:
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
        if self.config.metric in _MINIMIZE_METRICS:
            return a < b
        return a > b

    def _score_cv(
        self,
        df: pl.DataFrame,
        splits: list[tuple[list[int], list[int]]],
        match_level: list[str],
        point_level: list[str],
    ) -> float:
        if self.config.metric in _CHAIN_METRICS:
            return self._score_cv_chain(match_level, point_level)
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
        df = df.sort(["match_uid", "player_id"]).unique(
            subset=["match_uid"], keep="first", maintain_order=True,
        )

        # Materialize all candidate match-level features from cache.
        # ScoreStateChainServeModel.predict_state_fn reads `player_X` for
        # the server-side value and either `opp_X` (mirror=True features)
        # or the negated `player_X` (mirror=False / diff features) for the
        # swap-side. load_features_numpy materializes the opp_ column only
        # when the base feature mirrors, which matches that contract.
        df = engine.load_features_numpy(match_pool, df, cache_key)

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
        )
        splits = list(splitter.split(df))

        self._match_df = df
        self._match_splits = splits
        logger.info(
            "Chain-metric path: match-grain df=%d matches, %d folds, %d candidate match feats materialized",
            len(df), len(splits), len(match_pool),
        )

    def _score_cv_chain(
        self, match_level: list[str], point_level: list[str],
    ) -> float:
        assert self._match_df is not None and self._match_splits is not None, (
            "_score_cv_chain called before _prepare_match_data"
        )
        if not match_level and not point_level:
            return float("inf")
        fold_scores: list[float] = []
        for train_idx, test_idx in self._match_splits:
            train_df = self._match_df[train_idx]
            test_df = self._match_df[test_idx]

            model = ScoreStateChainServeModel(
                model_type=self.config.scoring_model.type,
                match_level_features=list(match_level),
                point_level_features=list(point_level),
                params=dict(self.config.scoring_model.params),
                points_path=self.points_path,
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
                engine=self._engine,
            )
            model.fit(train_df)
            p_a_fn, p_b_fn = model.predict_state_fn(test_df)
            p_a, p_b = model.predict(test_df)
            best_of = test_df["best_of"].to_numpy().astype(np.int64)
            dist = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)

            y_games_a = test_df["_target_games_a"].to_numpy().astype(np.float64)
            y_games_b = test_df["_target_games_b"].to_numpy().astype(np.float64)
            fold_scores.append(
                _score_dist_metric(self.config.metric, dist, y_games_a, y_games_b)
            )
        return float(np.mean(fold_scores))

    def _final_train_eval(
        self,
        df: pl.DataFrame,
        splits: list[tuple[list[int], list[int]]],
        match_level: list[str],
        point_level: list[str],
        form: str,
        params: dict[str, Any],
    ) -> tuple[dict[str, float], dict[str, Any] | None]:
        if self.config.metric in _CHAIN_METRICS:
            return self._final_train_eval_chain(match_level, point_level, form, params)
        feature_cols = self._resolve_cols(match_level, point_level)
        fold_metrics: list[dict[str, float]] = []
        for train_idx, test_idx in splits:
            train_df = df[train_idx]
            test_df = df[test_idx]
            X_train = train_df.select(feature_cols).to_numpy()
            y_train = train_df["point_won_by_server"].cast(pl.Int64).to_numpy()
            X_test = test_df.select(feature_cols).to_numpy()
            y_test = test_df["point_won_by_server"].cast(pl.Int64).to_numpy()
            model = build_score_state_model(type_=form, feature_names=feature_cols, params=params)
            model.fit(X_train, y_train)
            y_prob = model.predict_proba(X_test)
            fold_metrics.append(compute_metrics(y_test, y_prob))

        agg: dict[str, float] = {}
        if fold_metrics:
            for k in fold_metrics[0]:
                vals = [m[k] for m in fold_metrics if k in m]
                agg[k] = float(np.mean(vals))

        # Final fit for coef summary
        X_full = df.select(feature_cols).to_numpy()
        y_full = df["point_won_by_server"].cast(pl.Int64).to_numpy()
        model = build_score_state_model(type_=form, feature_names=feature_cols, params=params)
        model.fit(X_full, y_full)
        return agg, model.coef_summary()

    def _final_train_eval_chain(
        self,
        match_level: list[str],
        point_level: list[str],
        form: str,
        params: dict[str, Any],
    ) -> tuple[dict[str, float], dict[str, Any] | None]:
        """Chain-grain final eval: emits all four chain metrics per fold."""
        assert self._match_df is not None and self._match_splits is not None, (
            "_final_train_eval_chain called before _prepare_match_data"
        )
        fold_metrics: list[dict[str, float]] = []
        for train_idx, test_idx in self._match_splits:
            train_df = self._match_df[train_idx]
            test_df = self._match_df[test_idx]

            model = ScoreStateChainServeModel(
                model_type=form,
                match_level_features=list(match_level),
                point_level_features=list(point_level),
                params=dict(params),
                points_path=self.points_path,
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
                engine=self._engine,
            )
            model.fit(train_df)
            p_a_fn, p_b_fn = model.predict_state_fn(test_df)
            p_a, p_b = model.predict(test_df)
            best_of = test_df["best_of"].to_numpy().astype(np.int64)
            dist = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)
            y_games_a = test_df["_target_games_a"].to_numpy().astype(np.float64)
            y_games_b = test_df["_target_games_b"].to_numpy().astype(np.float64)
            fold_metrics.append({
                m: _score_dist_metric(m, dist, y_games_a, y_games_b)
                for m in _CHAIN_METRICS
            })

        agg: dict[str, float] = {}
        if fold_metrics:
            for k in fold_metrics[0]:
                agg[k] = float(np.mean([m[k] for m in fold_metrics]))
        return agg, None

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
        )

    def _pre_cache_all(
        self,
        *,
        base_match: list[str],
        candidate_match: list[str],
    ) -> tuple[FeatureEngine, str]:
        """Cache all match-level specs to disk without loading them into memory."""
        all_match_specs: list[str] = []
        for spec in base_match + candidate_match:
            if spec not in all_match_specs:
                all_match_specs.append(spec)
        for spec in get_filter_feature_specs(self.config.data.filters):
            if spec not in all_match_specs:
                all_match_specs.append(spec)

        extra_columns = ["circuit", "surface", "round", "best_of"]
        for col in self.config.data.filters:
            if col not in extra_columns:
                extra_columns.append(col)

        engine = FeatureEngine(matches_path=self.matches_path, cache_dir=self.cache_dir)
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
