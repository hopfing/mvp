"""The lead's out-of-sample probability, as a joinable feature.

Joined from `stage/model/lead_prior.parquet` (built by
`scripts/build_lead_prior.py <fingerprint>`) rather than computed: it is the
lead's own prediction for a match, and reconstructing it needs the lead's fold
artifacts, which have nothing to do with the feature engine.

`player_lead_logit` is the log-odds that THIS row's player wins, from a lead
model that never saw the match (temporal-CV fold for 2023-2025, backtest fold
for 2026). It exists to be supplied as `offset.feature` -- a frozen starting
log-odds a residual stage fits on top of -- but it is an ordinary column and
usable as a plain feature.

No `opp_` counterpart: the lead odd-projects its two orientations so they sum
to 1, and the opponent's logit is exactly the negation.

NULL WHERE THE LEAD NEVER SCORED THE MATCH OUT OF SAMPLE. A left join,
deliberately: `impute=None` (register_transform fixes this) so a match outside
the OOF span stays null instead of being handed the median. Trees take nulls
natively; the offset's logistic does not, so a config offsetting on this must
restrict its rows with `filters: {player_lead_logit: not_null}`. At serve time
pending matches have no row here either -- the predictor fills in the lead's
live logit before a stage scores them.

The parquet is a snapshot of ONE lead. `scripts/build_lead_prior.py` records
which fingerprint in `lead_prior.json` beside it and invalidates this
transform's engine cache, because a cached column group from a previous lead
would be silently wrong.
"""

from __future__ import annotations

import polars as pl

from mvp.common.base_job import get_data_root
from mvp.model.registry import register_transform

_OUTPUTS = ["player_lead_prob", "player_lead_logit"]
PRIOR_PATH_NAME = "lead_prior.parquet"


def prior_path():
    return get_data_root() / "stage" / "model" / PRIOR_PATH_NAME


def _lead_prior_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Engine transform: the lead prior keyed to each (match_uid, player_id).

    Returns just the keyed outputs, which the engine merges and caches -- the
    same contract as `market._market_transform`.
    """
    path = prior_path()
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing. Build it with "
            f"scripts/build_lead_prior.py <fingerprint>."
        )
    prior = pl.read_parquet(path).select(
        "match_uid", "player_id",
        pl.col("lead_prob").alias("player_lead_prob"),
        pl.col("lead_logit").alias("player_lead_logit"),
    )
    return (
        df.select("match_uid", "player_id")
        .join(prior, on=["match_uid", "player_id"], how="left")
        .select("match_uid", "player_id", *_OUTPUTS)
    )


register_transform(
    name="lead_prior",
    func=_lead_prior_transform,
    outputs=_OUTPUTS,
    description="The lead's out-of-sample win probability, as prob and log-odds",
)
