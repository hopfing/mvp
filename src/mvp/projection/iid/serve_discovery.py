"""Forward-selection discovery for the score-state serve model.

Precomputes the full feature matrix once (match-level from FeatureEngine +
point-level from match_beats_points + derived), then iteratively adds the
candidate feature whose inclusion most improves the CV metric. After FS
terminates, optionally re-trains all configured `model_forms` on the final
feature set for comparison.
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
from mvp.model.metrics import compute_metrics
from mvp.model.splitters import make_splitter
from mvp.projection.iid.config import ServeDiscoveryConfig
from mvp.projection.iid.score_state_features import add_derived_point_features
from mvp.projection.iid.score_state_model import build_score_state_model

logger = logging.getLogger(__name__)


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
    ) -> None:
        self.config_path = Path(config_path)
        self.config = ServeDiscoveryConfig.from_file(config_path)
        self.points_path = Path(points_path) if points_path else get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        self.matches_path = Path(matches_path) if matches_path else get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        self.cache_dir = Path(cache_dir) if cache_dir else get_local_data_root() / "features" / "cache"
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.run_name = run_name or self.config_path.stem

    def run(self) -> DiscoveryResult:
        df = self._build_full_matrix()
        logger.info("FS matrix: %d rows, %d columns", len(df), len(df.columns))

        splitter = self._make_splitter()
        splits = list(splitter.split(df))
        logger.info("FS splits: %d folds", len(splits))

        selected_match = list(self.config.features.base_match_level_features)
        selected_point = list(self.config.features.base_point_level_features)
        candidate_match = [c for c in self.config.features.candidate_match_level_features if c not in selected_match]
        candidate_point = [c for c in self.config.features.candidate_point_level_features if c not in selected_point]

        # Attempt to restore from checkpoint
        cp = self._load_checkpoint() if self.checkpoint_path else None
        rounds: list[FSRoundResult] = []
        partial_round_scores: dict[str, float] = {}

        if cp is not None:
            # Replay completed rounds
            for entry in cp.completed_rounds:
                feat = entry["feature"]
                grain = entry["grain"]
                score = entry["score"]
                if grain == "match":
                    selected_match.append(feat)
                    if feat in candidate_match:
                        candidate_match.remove(feat)
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
            current_score = self._score_cv(df, splits, selected_match, selected_point)
            logger.info("Base-only CV %s = %.6f (%d features)", self.config.metric, current_score, len(selected_match) + len(selected_point))
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

        while True:
            if self.config.features.max_features is not None:
                n_added = len([r for r in rounds if r.feature_added is not None])
                if n_added >= self.config.features.max_features:
                    break
            if not candidate_match and not candidate_point:
                break

            from tqdm import tqdm

            best_delta = -math.inf
            best_cand: str | None = None
            best_grain: str | None = None
            best_new_score = current_score

            tagged = [("match", c) for c in candidate_match] + [("point", c) for c in candidate_point]
            total_cands = len(tagged)
            cap = self.config.features.max_features
            desc = f"Round {round_idx}" + (f"/{cap}" if cap else "")
            bar = tqdm(tagged, desc=desc, leave=False, ncols=120)

            this_round_scores: dict[str, float] = dict(partial_round_scores)
            partial_round_scores = {}

            # Seed bar postfix from partial scores if any
            if this_round_scores:
                best_prev_cand = min(this_round_scores, key=this_round_scores.get) if self.config.metric in ("log_loss", "brier_score") else max(this_round_scores, key=this_round_scores.get)
                cand_score = this_round_scores[best_prev_cand]
                cand_delta = self._improvement(current_score, cand_score)
                if cand_delta > best_delta:
                    best_delta = cand_delta
                    best_cand = best_prev_cand
                    best_grain = "match" if best_prev_cand in candidate_match else "point"
                    best_new_score = cand_score

            for grain, cand in bar:
                if cand in this_round_scores:
                    score = this_round_scores[cand]
                else:
                    if grain == "match":
                        score = self._score_cv(df, splits, selected_match + [cand], selected_point)
                    else:
                        score = self._score_cv(df, splits, selected_match, selected_point + [cand])
                    this_round_scores[cand] = score
                    if self.checkpoint_path:
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
                delta = self._improvement(current_score, score)
                if delta > best_delta:
                    best_delta = delta
                    best_cand = cand
                    best_grain = grain
                    best_new_score = score
                    if hasattr(bar, "set_postfix"):
                        bar.set_postfix(best=f"{best_new_score:.6f}", feat=f"{cand}[{grain}]", refresh=False)

            if best_cand is None or best_delta < self.config.min_delta:
                logger.info("FS halting: no candidate exceeds min_delta=%.6f (best=%.6f)", self.config.min_delta, best_delta)
                break

            if best_grain == "match":
                selected_match.append(best_cand)
                candidate_match.remove(best_cand)
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

        # Final: train all configured forms on the selected feature set, report metrics
        final_forms: list[FinalFormResult] = []
        for form in self.config.model_forms:
            params = self.config.model_params.get(form, {})
            metrics, coefs = self._final_train_eval(df, splits, selected_match, selected_point, form, params)
            final_forms.append(FinalFormResult(form=form, metrics=metrics, coef_summary=coefs))
            logger.info("Final form %s: %s=%.6f", form, self.config.metric, metrics.get(self.config.metric, float("nan")))

        return DiscoveryResult(
            selected_match_level=selected_match,
            selected_point_level=selected_point,
            rounds=rounds,
            final_forms=final_forms,
            n_train_rows=len(df),
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
        direction = "minimize" if self.config.metric in ("log_loss", "brier_score") else "maximize"
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
        """Positive = better. For lower-is-better metrics (log_loss, brier), flip sign."""
        if self.config.metric in ("log_loss", "brier_score"):
            return current - new
        # roc_auc: higher is better
        return new - current

    def _score_cv(
        self,
        df: pl.DataFrame,
        splits: list[tuple[list[int], list[int]]],
        match_level: list[str],
        point_level: list[str],
    ) -> float:
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

    def _final_train_eval(
        self,
        df: pl.DataFrame,
        splits: list[tuple[list[int], list[int]]],
        match_level: list[str],
        point_level: list[str],
        form: str,
        params: dict[str, Any],
    ) -> tuple[dict[str, float], dict[str, Any] | None]:
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

    def _make_splitter(self) -> Any:
        val = self.config.validation
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

    def _build_full_matrix(self) -> pl.DataFrame:
        points = pl.read_parquet(self.points_path)
        logger.info("Loaded %d point rows", len(points))

        # All match-level specs needed: base + candidates + filter-referenced
        all_match_specs: list[str] = []
        for spec in list(self.config.features.base_match_level_features) + list(self.config.features.candidate_match_level_features):
            if spec not in all_match_specs:
                all_match_specs.append(spec)
        for spec in get_filter_feature_specs(self.config.data.filters):
            if spec not in all_match_specs:
                all_match_specs.append(spec)

        engine = FeatureEngine(matches_path=self.matches_path, cache_dir=self.cache_dir)
        matches_features = engine.compute(
            feature_specs=all_match_specs,
            extra_columns=["player_id", "opp_id", "match_uid"],
        )

        joined = points.join(
            matches_features.rename({"player_id": "server_id", "opp_id": "returner_id"}),
            on=["match_uid", "server_id", "returner_id"],
            how="inner",
        )
        renames: dict[str, str] = {}
        for col in joined.columns:
            if col.startswith("player_") and col != "player_id":
                renames[col] = "server_" + col[len("player_"):]
            elif col.startswith("opp_") and col != "opp_id":
                renames[col] = "returner_" + col[len("opp_"):]
        if renames:
            joined = joined.rename(renames)

        # Date filter
        start = self.config.data.date_range.start if self.config.data.date_range else None
        end = self.config.data.date_range.end if self.config.data.date_range else None
        if start is not None:
            joined = joined.filter(pl.col("effective_match_date") >= start)
        if end is not None:
            joined = joined.filter(pl.col("effective_match_date") <= end)

        # Add draw_type literal for filter compatibility
        if "draw_type" not in joined.columns:
            joined = joined.with_columns(pl.lit("singles").alias("draw_type"))
        joined = apply_filters(joined, self.config.data.filters)
        joined = joined.filter(pl.col("point_won_by_server").is_not_null())

        # Precompute all candidate point-level derived features
        all_point_names: list[str] = list(self.config.features.base_point_level_features)
        for f in self.config.features.candidate_point_level_features:
            if f not in all_point_names:
                all_point_names.append(f)
        joined = add_derived_point_features(joined, all_point_names)

        return joined
