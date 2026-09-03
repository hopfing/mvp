"""The bettable market's opening price, as a model feature.

Joined from `stage/oddspapi/market_prior.parquet` (built by
`scripts/oddspapi/build_market_prior.py`) rather than computed: the price is an
observation, not a function of match history, and reconstructing it needs the
tick stream and an anchor sweep that have nothing to do with the feature engine.

`player_market_logit` is the de-vigged log-odds that THIS row's player wins, at
the first moment an entry book was two-sided. It exists to be supplied as
`offset.feature` -- a starting log-odds the trees fit residual to -- but it is an
ordinary column and usable as a plain feature.

There is no `opp_` counterpart. De-vigging makes the two sides sum to 1, so the
opponent's logit is exactly the negation and a second column would be a
collinear duplicate.

NULL WHERE NO PRICE. A left join, deliberately: `impute=None` (register_transform
fixes this) so a match nobody priced stays null instead of being handed the
median. Trees take nulls natively; the offset's LogisticRegression does not, so a
config offsetting on this must restrict its rows to priced matches.
"""

from __future__ import annotations

import polars as pl

from mvp.model.registry import register_transform
from mvp.oddspapi import paths

_OUTPUTS = ["player_market_prob", "player_market_logit"]

PRIOR_PATH_NAME = "market_prior.parquet"


def _market_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Engine transform: the market prior keyed to each (match_uid, player_id).

    Returns just the keyed outputs, which the engine merges and caches -- the
    same contract as `style_matchup_retrieval._matchup_transform`.
    """
    path = paths.stage_root() / PRIOR_PATH_NAME
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing. Build it with "
            f"scripts/oddspapi/build_market_prior.py --book <book>."
        )
    prior = pl.read_parquet(path).rename(
        {"market_prob": "player_market_prob", "market_logit": "player_market_logit"}
    )
    return (
        df.select("match_uid", "player_id")
        .join(prior, on=["match_uid", "player_id"], how="left")
        .select("match_uid", "player_id", *_OUTPUTS)
    )


def _market_salt() -> str:
    """Freshness of the external artifact this transform reads. Without a
    salt the cache entry would outlive rebuilds of market_prior.parquet —
    under per-spec invalidation there is no accidental global wipe left to
    save it (granular-cache plan, round-3 finding)."""
    p = paths.stage_root() / PRIOR_PATH_NAME
    return str(int(p.stat().st_mtime)) if p.exists() else "-"


register_transform(
    name="market_prior",
    func=_market_transform,
    outputs=_OUTPUTS,
    cache_salt=_market_salt,
    description="Entry-book opening moneyline, de-vigged, as prob and log-odds",
)
