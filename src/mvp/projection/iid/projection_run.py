"""Train an IID projector, project 2026, and write the per-match total-games pmf.

This is the half of the old `backtest.py` that has nothing to do with odds. It was
worth separating because the two halves answer different questions and change for
different reasons: this one depends on the feature engine, the serve model and the
chain; the odds half depends on what books offered. Keeping them in one file meant the
only pmf producer in the repo lived inside a module named for the thing that consumed
it.

**`test_df` and `ProjectionOutput` are positionally aligned.** `out.distribution`
indexes by row order of the frame passed to `project`, so a caller that rebuilds the
test set separately and pairs it with a cached projection gets silent row-wise
mismatch. `run_projection` therefore returns them together, in one object, and callers
should never reconstruct either half on its own.

**The test set is not gated on scraper coverage.** `_build_test_set` can inner-join
`odds/event_map.parquet`, which would let the retired scraper source govern which
matches are evaluated. The driver passes `require_odds_coverage=False`: project every
settleable 2026 match and let the odds layer decide what is priceable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import make_fs_engine
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.projection.iid.artifacts import (
    SERVE_MODEL_JOBLIB,
    fp_dir_for,
    record_run,
    write_pmf_parquet,
)
from mvp.projection.iid.config import IIDProjectionConfig
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import build_serve_model

logger = logging.getLogger(__name__)

_RUNNER_COLUMNS = [
    "match_uid", "player_id", "won", "reason", "best_of",
    "circuit", "surface", "round", "effective_match_date",
    "player_set1_games", "player_set2_games",
    "player_set3_games", "player_set4_games", "player_set5_games",
    "opp_set1_games", "opp_set2_games",
    "opp_set3_games", "opp_set4_games", "opp_set5_games",
    "pts_service_pts_won", "pts_service_pts_played",
    "opp_pts_service_pts_won", "opp_pts_service_pts_played",
    "svc_games_played", "svc_bp_saved", "svc_bp_faced",
    "opp_svc_games_played", "opp_svc_bp_saved", "opp_svc_bp_faced",
    "player_set1_tiebreak", "player_set2_tiebreak",
    "player_set3_tiebreak", "player_set4_tiebreak", "player_set5_tiebreak",
    "opp_set1_tiebreak", "opp_set2_tiebreak",
    "opp_set3_tiebreak", "opp_set4_tiebreak", "opp_set5_tiebreak",
    "player_first_name", "player_last_name",
    "opp_first_name", "opp_last_name", "opp_id",
    "tournament_id", "tournament_name",
]


@dataclass(frozen=True)
class ProjectionRun:
    """A projection and the frame it was computed over, kept together.

    Splitting these is the failure mode this class exists to prevent: the output
    indexes `test_df` by position, and nothing raises if they disagree.
    """

    fp_dir: Path
    config: IIDProjectionConfig
    config_path: Path
    test_df: pl.DataFrame
    output: ProjectionOutput
    pmf: pl.DataFrame


def artifact_path(config: IIDProjectionConfig, config_path: Path) -> Path:
    """Trained-projector cache, keyed by config CONTENT rather than filename stem.

    Stem-keying collided: a sweep over N hyperparameter variants of one config wrote
    every variant to the same `<stem>.joblib`, so each run silently overwrote the last
    and the comparison was between one model and itself.
    """
    return fp_dir_for(config, config_path) / SERVE_MODEL_JOBLIB


def resolve_targets(df: pl.DataFrame) -> pl.DataFrame:
    """Drop matches whose total-games target is not meaningful.

    A retirement, walkover, default or unplayed match produces fewer games than the
    match would have, so scoring one against a total records an "under" no book would
    have settled that way. This is why the pmf's `actual_total` can be trusted as the
    settlement source downstream.
    """
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
    return df.filter(
        pl.col("_target_games_a").is_not_null()
        & pl.col("_target_games_b").is_not_null()
    )


def _collapse_to_match_rows(df: pl.DataFrame) -> pl.DataFrame:
    """One row per match, lower `player_id` first.

    This defines the a/b convention the whole projection side is expressed in. It is
    NOT our p1/p2 convention, and anything joining prices has to map between them.
    """
    return df.sort(["match_uid", "player_id"]).unique(
        subset=["match_uid"], keep="first", maintain_order=True,
    )


def compute_features(config: IIDProjectionConfig) -> tuple[pl.DataFrame, Any]:
    """One feature-engine pass, shared by training and the test set."""
    matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
    cache_dir = get_local_data_root() / "features" / "cache"
    engine = make_fs_engine(matches_path=matches_path, cache_dir=cache_dir)

    feature_specs = config.features.include
    compute_only = config.features.compute_only or []
    filter_specs = get_filter_feature_specs(config.data.filters)
    extra = compute_only + filter_specs
    all_specs = feature_specs + [s for s in extra if s not in feature_specs]

    runner_columns = list(_RUNNER_COLUMNS)
    if config.data.filters:
        for col in config.data.filters:
            if col not in runner_columns:
                runner_columns.append(col)

    return engine.compute(all_specs, extra_columns=runner_columns), engine


def _train_projector(
    config: IIDProjectionConfig, df: pl.DataFrame, engine: Any = None,
) -> TennisProjector:
    train_df = df
    if config.data.filters:
        train_df = apply_filters(train_df, config.data.filters)
    train_df = resolve_targets(train_df)
    train_df = train_df.filter(
        (pl.col("effective_match_date") >= config.data.date_range.start)
        & (pl.col("effective_match_date") <= config.data.date_range.end)
    )
    train_df = train_df.filter(pl.col("best_of").is_in([3, 5]))
    train_df = _collapse_to_match_rows(train_df)
    if len(train_df) == 0:
        raise ValueError("No training matches after filters")
    logger.info("Training projector on %d matches", len(train_df))
    projector = TennisProjector(build_serve_model(config.serve_model, engine=engine))
    projector.fit(train_df)
    return projector


def _save_artifact(
    projector: TennisProjector, config: IIDProjectionConfig, config_path: Path,
    n_train: int,
) -> None:
    path = artifact_path(config, config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "serve_model": projector.serve_model,
            "config_path": str(config_path),
            "config_yaml": Path(config_path).read_text(encoding="utf-8"),
            "n_train": n_train,
            "trained_at": datetime.now(UTC).isoformat(),
        },
        path,
    )
    logger.info("Saved IID artifact to %s", path)


def _load_artifact(
    config: IIDProjectionConfig, config_path: Path,
) -> TennisProjector | None:
    """The cached projector, or None if it does not match the current config text.

    The fingerprint keys the dir by content, so a mismatch should be rare — but the
    artifact stores the config text it was trained from, and fields outside the
    fingerprint (or a hand-edited file) can still diverge. Without this check, editing
    a config and re-running without `retrain` silently scores the OLD model.
    """
    path = artifact_path(config, config_path)
    if not path.exists():
        return None
    artifact = joblib.load(path)
    if artifact.get("config_yaml") != Path(config_path).read_text(encoding="utf-8"):
        logger.info(
            "IID artifact at %s was trained from different config text — retraining",
            path,
        )
        return None
    return TennisProjector(serve_model=artifact["serve_model"])


def train_or_load(
    config: IIDProjectionConfig, config_path: Path, df: pl.DataFrame,
    *, retrain: bool, engine: Any = None,
) -> TennisProjector:
    if not retrain:
        cached = _load_artifact(config, config_path)
        if cached is not None:
            logger.info("Loading existing IID artifact for %s", config_path.stem)
            return cached
    projector = _train_projector(config, df, engine=engine)
    _save_artifact(projector, config, config_path, n_train=len(df))
    return projector


def build_test_set(
    config: IIDProjectionConfig,
    df: pl.DataFrame,
    *,
    require_odds_coverage: bool = False,
) -> pl.DataFrame:
    """Settled 2026 matches, one row per match.

    `require_odds_coverage=True` inner-joins the SCRAPER `odds/event_map.parquet`,
    which lets a retired source govern which matches are evaluated. Default False:
    project everything settleable and let the odds layer decide what is priceable.
    """
    test = df
    if config.data.filters:
        test = apply_filters(test, config.data.filters)
    test = resolve_targets(test)
    test = test.filter(pl.col("effective_match_date") >= pl.date(2026, 1, 1))
    test = test.filter(pl.col("best_of").is_in([3, 5]))
    test = _collapse_to_match_rows(test)

    if not require_odds_coverage:
        return test
    covered = pl.read_parquet(
        get_data_root() / "odds" / "event_map.parquet"
    ).select("match_uid").unique()
    return test.join(covered, on="match_uid", how="inner")


def build_pmf_frame(test_df: pl.DataFrame, out: ProjectionOutput) -> pl.DataFrame:
    """Per-match total-games pmf — the model's whole opinion, per match.

    `total_games_pmf` is indexed BY GAME COUNT: element i is P(total == i games).
    Aligned to `test_df` positionally, which is why the two travel together.

    The alignment is ASSERTED, not assumed. Taking identity from `test_df` and
    probabilities from `out.distribution` independently means an equal-length but
    reordered pair writes a silently wrong pmf — every match gets another match's
    distribution, and nothing raises. `ProjectionOutput` carries its own `match_uid`
    (`projector.py:35`), so the check costs one comparison.
    """
    if not (test_df["match_uid"].to_numpy() == out.match_uid).all():
        raise ValueError(
            "test_df and ProjectionOutput are not row-aligned; the pmf would pair "
            "each match with another match's distribution"
        )
    return pl.DataFrame({
        "match_uid": test_df["match_uid"],
        "circuit": test_df["circuit"],
        "surface": test_df["surface"],
        "round": test_df["round"],
        "best_of": test_df["best_of"].cast(pl.Int64),
        "actual_total": (
            test_df["_target_games_a"] + test_df["_target_games_b"]
        ).cast(pl.Float64),
        "p_match_win_a": out.distribution.p_match_win_a,
        "expected_total_games": out.distribution.expected_total_games,
        "total_games_pmf": [
            row.tolist() for row in out.distribution.total_games_pmf
        ],
    })


def run_projection(
    config_path: Path | str,
    *,
    retrain: bool = False,
    source: str | None = None,
    run_id: str | None = None,
) -> ProjectionRun:
    """Train (or load), project 2026, write the pmf. Returns everything together.

    `source` / `run_id` group this run under a parent config in the fingerprint dir's
    source.txt — a sweep passes its parent stem so `iid-rank` can show variants
    together. It is also what creates the fingerprint dir for a bare run of a config
    that has never been through `iid-project`.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    config = IIDProjectionConfig.from_file(str(config_path))
    record_run(config, config_path, source=source, run_id=run_id)

    df, engine = compute_features(config)
    projector = train_or_load(config, config_path, df, retrain=retrain, engine=engine)

    test_df = build_test_set(config, df)
    if len(test_df) == 0:
        raise RuntimeError("No settled 2026 matches to project")
    logger.info("Projecting %d 2026 matches", len(test_df))
    out = projector.project(test_df)

    fp_dir = fp_dir_for(config, config_path)
    pmf = build_pmf_frame(test_df, out)
    logger.info("Wrote per-match total-games pmf -> %s", write_pmf_parquet(fp_dir, pmf))

    return ProjectionRun(
        fp_dir=fp_dir,
        config=config,
        config_path=config_path,
        test_df=test_df,
        output=out,
        pmf=pmf,
    )
