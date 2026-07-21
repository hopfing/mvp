"""Shared candidate-loop parallelism for the feature-selection paths.

Both the classification FS (``mvp.model.discovery.discover``) and the score-state
projection FS (``mvp.projection.iid.serve_discovery``) split a thread budget into
``workers`` concurrent candidate fits x ``n_jobs`` threads/fit, and cap BLAS
threads for models whose fit ignores ``n_jobs``. This module is the single source
of that logic so the two paths can't drift.
"""

from __future__ import annotations

import os
from contextlib import AbstractContextManager, nullcontext

from threadpoolctl import threadpool_limits

from mvp.model.models import get_n_jobs_override

# Model types whose fit ignores sklearn ``n_jobs`` and spends its time in BLAS
# (e.g. LogisticRegression's default lbfgs solver on a binary target). For these
# the per-fit thread share is applied as a BLAS thread cap via threadpoolctl,
# since ``n_jobs`` on the model is a no-op. XGB routes n_jobs through OpenMP, not
# BLAS, so it is unaffected by the cap.
BLAS_THREADED_MODEL_TYPES = {"logistic"}


def resolve_candidate_parallelism(
    cfg_n_jobs: int | None,
    forward_max_workers: int | None,
    *,
    cpu: int | None = None,
) -> tuple[int, int]:
    """Split a thread budget into ``(workers, n_jobs_per_fit)`` for the loop.

    Budget = ``cfg_n_jobs``, else the ``--n-jobs`` override, else ``cpu - 2``;
    capped at ``cpu``. ``workers`` = ``forward_max_workers`` if set, else
    ``budget // 4`` (~4 threads/fit, the xgb knee). ``n_jobs_per_fit`` =
    ``budget // workers``. Callers gate non-forward/serial paths themselves.
    """
    cpu = cpu or os.cpu_count() or 4
    budget = (
        int(cfg_n_jobs) if cfg_n_jobs
        else (get_n_jobs_override() or max(1, cpu - 2))
    )
    budget = max(1, min(int(budget), cpu))
    workers = (
        max(1, forward_max_workers) if forward_max_workers is not None
        else max(1, budget // 4)
    )
    workers = max(1, min(workers, budget))
    n_jobs = max(1, budget // workers)
    return workers, n_jobs


def blas_thread_cap(model_type: str, n_jobs: int | None) -> AbstractContextManager:
    """BLAS thread cap for BLAS-threaded models; no-op otherwise.

    Apply ONCE around the whole candidate loop, not per-fit: threadpoolctl's BLAS
    limit is process-global, so a per-fit context manager would race on restore
    between concurrent workers. Returns ``nullcontext()`` for models that route
    their own threads (xgb via OpenMP) or when ``n_jobs`` is None (serial paths).
    """
    if model_type in BLAS_THREADED_MODEL_TYPES and n_jobs is not None:
        return threadpool_limits(limits=n_jobs)
    return nullcontext()
