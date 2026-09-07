"""Serve/return skill filter columns (mvp.atptour.bsr) as model features.

Pre-match passthroughs per side, all null-preserving: a player with no
observation on a stream yet has no state, and that absence is information a
tree reads through the sd and count columns rather than a median.

Two blocks. The twelve shipped names of the pooled `serve` stream are written
out one by one below, because the serve feature-selection runs and the three
`serve_base_*` configs reference them and a rename would be silent. The other
20 streams' 178 names are generated from `BSR_NEW_VALUE_NAMES`, which the
filter derives from the same stream table the ratings pass emits from — so a
stream cannot gain a column that has no feature, or keep a feature whose
column is gone.
"""

import polars as pl

from mvp.atptour.bsr.filter import BSR_NEW_VALUE_NAMES
from mvp.model.registry import feature, register_diff, register_sum


@feature(
    name="bsr_serve_mu",
    description="Serve skill posterior mean (logit scale)",
    mirror=True,
    impute=None,
)
def bsr_serve_mu() -> pl.Expr:
    return pl.col("player_bsr_serve_mu")


@feature(
    name="bsr_serve_sd",
    description="Serve skill posterior sd",
    mirror=True,
    impute=None,
)
def bsr_serve_sd() -> pl.Expr:
    return pl.col("player_bsr_serve_sd")


@feature(
    name="bsr_return_mu",
    description="Return skill posterior mean (logit scale)",
    mirror=True,
    impute=None,
)
def bsr_return_mu() -> pl.Expr:
    return pl.col("player_bsr_return_mu")


@feature(
    name="bsr_return_sd",
    description="Return skill posterior sd",
    mirror=True,
    impute=None,
)
def bsr_return_sd() -> pl.Expr:
    return pl.col("player_bsr_return_sd")


@feature(
    name="bsr_serve_surface_mu",
    description="Serve surface-residual posterior mean, this match's surface",
    mirror=True,
    impute=None,
)
def bsr_serve_surface_mu() -> pl.Expr:
    return pl.col("player_bsr_serve_surface_mu")


@feature(
    name="bsr_serve_surface_sd",
    description="Serve surface-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_serve_surface_sd() -> pl.Expr:
    return pl.col("player_bsr_serve_surface_sd")


@feature(
    name="bsr_return_surface_mu",
    description="Return surface-residual posterior mean, this match's surface",
    mirror=True,
    impute=None,
)
def bsr_return_surface_mu() -> pl.Expr:
    return pl.col("player_bsr_return_surface_mu")


@feature(
    name="bsr_return_surface_sd",
    description="Return surface-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_return_surface_sd() -> pl.Expr:
    return pl.col("player_bsr_return_surface_sd")


@feature(
    name="bsr_n_serve_obs",
    description="Serve-count observations the filter has seen for the player",
    mirror=True,
    impute=None,
)
def bsr_n_serve_obs() -> pl.Expr:
    return pl.col("player_bsr_n_serve_obs")


@feature(
    name="bsr_days_since_serve_obs",
    description="Days since the player's last serve-count observation",
    mirror=True,
    impute=None,
)
def bsr_days_since_serve_obs() -> pl.Expr:
    return pl.col("player_bsr_days_since_serve_obs")


@feature(
    name="bsr_pserve_logit",
    description="Matchup serve-point win logit vs this opponent, posterior mean",
    mirror=True,
    impute=None,
)
def bsr_pserve_logit() -> pl.Expr:
    return pl.col("player_bsr_pserve_logit")


@feature(
    name="bsr_pserve_logit_sd",
    description="Its posterior sd, including the per-match random effect",
    mirror=True,
    impute=None,
)
def bsr_pserve_logit_sd() -> pl.Expr:
    return pl.col("player_bsr_pserve_logit_sd")


@feature(
    name="bsr_serve_indoor_mu",
    description="Serve indoor-residual posterior mean (stored axis, every row)",
    mirror=True,
    impute=None,
)
def bsr_serve_indoor_mu() -> pl.Expr:
    return pl.col("player_bsr_serve_indoor_mu")


@feature(
    name="bsr_serve_indoor_sd",
    description="Serve indoor-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_serve_indoor_sd() -> pl.Expr:
    return pl.col("player_bsr_serve_indoor_sd")


@feature(
    name="bsr_return_indoor_mu",
    description="Return indoor-residual posterior mean (stored axis, every row)",
    mirror=True,
    impute=None,
)
def bsr_return_indoor_mu() -> pl.Expr:
    return pl.col("player_bsr_return_indoor_mu")


@feature(
    name="bsr_return_indoor_sd",
    description="Return indoor-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_return_indoor_sd() -> pl.Expr:
    return pl.col("player_bsr_return_indoor_sd")


for _base in (
    "bsr_serve_mu", "bsr_return_mu", "bsr_serve_surface_mu",
    "bsr_return_surface_mu", "bsr_pserve_logit",
    "bsr_serve_indoor_mu", "bsr_return_indoor_mu",
):
    register_diff(_base)
for _base in ("bsr_serve_mu", "bsr_return_mu", "bsr_pserve_logit"):
    register_sum(_base)


# --- the other 20 streams -------------------------------------------------
#
# Generated, not hand-written: 178 names, and the only thing that varies
# across them is the column they read. The description is assembled from the
# stream name and the suffix so the registry listing stays readable.

_SUFFIX_DESC = {
    "mu": "server-axis posterior mean (logit scale)",
    "sd": "server-axis posterior sd",
    "r_mu": "returner-axis posterior mean (logit scale)",
    "r_sd": "returner-axis posterior sd",
    "surface_mu": "surface-residual posterior mean, this match's surface",
    "surface_sd": "surface-residual posterior sd, this match's surface",
    "indoor_mu": "indoor-residual posterior mean",
    "indoor_sd": "indoor-residual posterior sd",
    "r_surface_mu": (
        "returner-axis surface-residual posterior mean, this match's surface"
    ),
    "r_surface_sd": (
        "returner-axis surface-residual posterior sd, this match's surface"
    ),
    "r_indoor_mu": "returner-axis indoor-residual posterior mean",
    "r_indoor_sd": "returner-axis indoor-residual posterior sd",
    "n_obs": "observations of this stream the filter has seen for the player",
    "days_since": "days since the player's last observation of this stream",
    "logit": "matchup logit vs this opponent, posterior mean",
    "logit_sd": "its posterior sd, including the per-match random effect",
}


def _passthrough(name: str) -> None:
    """Register `name` as a null-preserving, mirrored read of its column."""
    stream, suffix = _split(name)
    what = _SUFFIX_DESC[suffix]

    @feature(
        name=name,
        description=f"bsr {stream} stream: {what}",
        mirror=True,
        impute=None,
    )
    def _f(_col: str = f"player_{name}") -> pl.Expr:
        return pl.col(_col)


def _split(name: str) -> tuple[str, str]:
    """`bsr_rally_short_r_mu` -> ("rally_short", "r_mu").

    Longest suffix first so `surface_mu` never resolves as `mu` and leaves
    `..._surface` as the stream name.
    """
    body = name[len("bsr_"):]
    for suffix in sorted(_SUFFIX_DESC, key=len, reverse=True):
        if body.endswith("_" + suffix):
            return body[: -len(suffix) - 1], suffix
    raise ValueError(f"unrecognised bsr value name: {name}")


for _name in BSR_NEW_VALUE_NAMES:
    _passthrough(_name)

# Diffs on every mean — server, returner, surface, indoor — and on the matchup
# logit; sums on the means. Both inherit impute=None from the base, so a
# missing stream stays missing rather than being median-fabricated.
for _name in BSR_NEW_VALUE_NAMES:
    if _name.endswith("_mu") or _name.endswith("_logit"):
        register_diff(_name)
    if _name.endswith("_mu"):
        register_sum(_name)
