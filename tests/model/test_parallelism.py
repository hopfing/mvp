"""Pin the shared candidate-loop parallelism helper's behavior.

These reproduce the exact math the classification FS used inline before it was
extracted, so a regression in `resolve_candidate_parallelism` / `blas_thread_cap`
can't silently drift either FS path.
"""
from contextlib import nullcontext

from threadpoolctl import threadpool_limits

from mvp.model.parallelism import (
    BLAS_THREADED_MODEL_TYPES,
    blas_thread_cap,
    resolve_candidate_parallelism,
)


def test_explicit_workers_override():
    # forward_max_workers set → workers is that (capped at budget), n_jobs splits.
    workers, n_jobs = resolve_candidate_parallelism(12, 3, cpu=24)
    assert workers == 3
    assert n_jobs == 4  # budget 12 // 3 workers


def test_auto_workers_targets_four_threads_per_fit():
    # No override → workers = budget // 4 (the xgb knee).
    workers, n_jobs = resolve_candidate_parallelism(12, None, cpu=24)
    assert workers == 3      # 12 // 4
    assert n_jobs == 4       # 12 // 3


def test_budget_capped_at_cpu():
    workers, n_jobs = resolve_candidate_parallelism(100, None, cpu=8)
    assert workers * n_jobs <= 8


def test_budget_defaults_to_cpu_minus_two_when_unset(monkeypatch):
    # cfg_n_jobs None and no override → budget = cpu - 2.
    monkeypatch.setattr("mvp.model.parallelism.get_n_jobs_override", lambda: None)
    workers, n_jobs = resolve_candidate_parallelism(None, None, cpu=10)
    # budget 8 → workers 2 (8//4), n_jobs 4
    assert workers == 2
    assert n_jobs == 4


def test_workers_never_zero():
    workers, n_jobs = resolve_candidate_parallelism(1, None, cpu=1)
    assert workers >= 1
    assert n_jobs >= 1


def test_blas_cap_active_for_logistic():
    cap = blas_thread_cap("logistic", 4)
    assert isinstance(cap, type(threadpool_limits(limits=4)))


def test_blas_cap_noop_for_xgboost():
    assert isinstance(blas_thread_cap("xgboost", 4), type(nullcontext()))


def test_blas_cap_noop_when_n_jobs_none():
    assert isinstance(blas_thread_cap("logistic", None), type(nullcontext()))


def test_blas_set_contains_logistic_only():
    assert BLAS_THREADED_MODEL_TYPES == {"logistic"}
