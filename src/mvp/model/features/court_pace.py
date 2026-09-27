"""Court pace index: how fast a tournament edition's court plays, from serve outcomes.

Each server's serve points won are compared with what the serve-return rating
expected for that matchup (`player_bsr_pserve_logit`, pre-match). The
residuals are pooled per edition, keyed by (tournament, surface, indoor), from
earlier editions of the key (half-life one year) and from the lower rounds of
the same edition, shrunk toward the cell mean. Every contribution is first
corrected for its circuit's qualifying-versus-main-draw effect, so the index
does not move with how much qualifying an edition has.

The window is defined by `round_order`, never by date: historical
`effective_match_date` values are round-offset estimates, so a date window
would mean one thing in history and another live.

Outputs, all match-level (the same on both orientation rows of a match):

* ``court_pace``: serve points won per 100 above the rating's expectation,
  net of qualifying.
* ``court_pace_n``: the decayed serve points behind the value.
* ``court_pace_full``: ``court_pace`` plus the surface-and-indoor offset from
  the rating's own cell fixed effects, one ordered column that carries surface
  and court together.

Null outside scope: doubles, ITF, before the rating starts (2015), or with no
tournament id, year or round order. Plan and measurements:
mvp-docs/plans/2026-09-27-court-pace-index.md.

A second transform, ``court_pace_serve``, crosses ``court_pace`` and
``court_pace_full`` with four measures of serve power (the style serve axis,
the rating's ace skill, its serve-minus-return skill, first-serve speed), each
centred on its prior 730-day field mean, as ``player_``, ``opp_`` and
``player_..._diff`` columns: 8 stems, 24 outputs. Plan:
mvp-docs/plans/2026-09-27-court-pace-serve-interactions.md.
"""

from __future__ import annotations

import math

import polars as pl

from mvp.atptour.bsr.constants import CIRCUIT_INDEX, MU_CELLS, SURFACE_INDEX
from mvp.model.registry import register_transform

HALF_LIFE_YEARS = 1.0
PRIOR_STRENGTH = 2000.0
CELL_YEARS = 3
START_YEAR = 2015
N_ROUNDS = 12
QUAL_MAX_ROUND = 3

_OUTPUTS = ["court_pace", "court_pace_n", "court_pace_full"]
_RAW = [
    "tournament_id",
    "year",
    "surface",
    "indoor",
    "circuit",
    "draw_type",
    "round_order",
    "svc_first_serve_pts_won",
    "svc_first_serve_pts_played",
    "svc_second_serve_pts_won",
    "svc_second_serve_pts_played",
    "player_bsr_pserve_logit",
]
FIELD_DAYS = 730
_SERVE_PACES = ["court_pace", "court_pace_full"]
_SERVE_ATTRS: dict[str, tuple[pl.Expr, pl.Expr]] = {
    "style_serve": (pl.col("player_style_axis_serve"), pl.col("opp_style_axis_serve")),
    "bsr_ace": (pl.col("player_bsr_ace_mu"), pl.col("opp_bsr_ace_mu")),
    "bsr_balance": (
        pl.col("player_bsr_serve_mu") - pl.col("player_bsr_return_mu"),
        pl.col("opp_bsr_serve_mu") - pl.col("opp_bsr_return_mu"),
    ),
    "serve_speed": (
        pl.col("player_style_avg_1st_serve_speed"),
        pl.col("opp_style_avg_1st_serve_speed"),
    ),
}
_SERVE_STEMS = [f"{p}_{k}" for k in _SERVE_ATTRS for p in _SERVE_PACES]
_SERVE_OUTPUTS = [f"{side}_{s}" for s in _SERVE_STEMS for side in ("player", "opp")] + [
    f"player_{s}_diff" for s in _SERVE_STEMS
]
_SERVE_RAW = list(
    dict.fromkeys(
        _RAW
        + [
            "effective_match_date",
            "player_bsr_ace_mu",
            "opp_bsr_ace_mu",
            "player_bsr_serve_mu",
            "opp_bsr_serve_mu",
            "player_bsr_return_mu",
            "opp_bsr_return_mu",
        ]
    )
)

_KEY = ["tournament_id", "s3", "ind"]
_CELL = ["s3", "circuit", "ind"]


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _mu(surface_idx: int, circuit_idx: int, indoor: int) -> float:
    return MU_CELLS[surface_idx * 4 + circuit_idx * 2 + indoor]


def _surface_offset(s3: str, indoor: bool) -> float:
    """The rating's cell fixed effect for (surface, indoor) with circuit averaged
    out, relative to outdoor hard, in serve points won per 100 at the
    hard-outdoor reference."""
    s = SURFACE_INDEX[s3]
    i = int(indoor)
    circuits = (CIRCUIT_INDEX["tour"], CIRCUIT_INDEX["chal"])
    hard = SURFACE_INDEX["Hard"]
    ref = sum(_mu(hard, c, 0) for c in circuits) / len(circuits)
    se = sum(_mu(s, c, i) - _mu(hard, c, 0) for c in circuits) / len(circuits)
    return 100.0 * (_sigmoid(ref + se) - _sigmoid(ref))


def _qualifying_profile(con: pl.DataFrame, years: pl.DataFrame) -> pl.DataFrame:
    """q(circuit, qual, year): the circuit's qualifying (or main-draw) residual rate
    over all years before ``year`` minus the circuit's rate over the same years.

    ``con`` carries ``circuit``, ``qual``, ``year``, ``R`` and ``P`` (any grain);
    ``years`` is the single-column frame of years to produce. Weighted by each
    circuit's prior serve points, q sums to zero."""
    qs = con.group_by(["circuit", "qual", "year"]).agg(
        pl.col("R").sum().alias("sR"), pl.col("P").sum().alias("sP")
    )
    q_all = (
        qs.select("circuit", "qual")
        .unique()
        .join(years, how="cross")
        .join(qs.rename({"year": "py"}), on=["circuit", "qual"])
        .filter(pl.col("py") < pl.col("year"))
        .group_by(["circuit", "qual", "year"])
        .agg(pl.col("sR").sum().alias("qR"), pl.col("sP").sum().alias("qP"))
    )
    q_circ = q_all.group_by(["circuit", "year"]).agg(
        pl.col("qR").sum().alias("aR"), pl.col("qP").sum().alias("aP")
    )
    return q_all.join(q_circ, on=["circuit", "year"]).select(
        "circuit",
        "qual",
        "year",
        (pl.col("qR") / pl.col("qP") - pl.col("aR") / pl.col("aP")).alias("q"),
    )


def _court_pace_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Engine transform: the three outputs keyed to each (match_uid, player_id)."""
    scope = (
        (pl.col("draw_type") == "singles")
        & pl.col("circuit").is_in(["tour", "chal"])
        & pl.col("tournament_id").is_not_null()
        & pl.col("year").is_not_null()
        & (pl.col("year") >= START_YEAR)
        & pl.col("round_order").is_not_null()
    )
    # The round grid covers 1..N_ROUNDS; a row outside it would silently get null.
    off_grid = df.filter(scope & ~pl.col("round_order").is_between(1, N_ROUNDS)).height
    if off_grid:
        raise ValueError(
            f"court_pace_index: {off_grid} in-scope rows with round_order outside "
            f"1..{N_ROUNDS}"
        )
    # Every surface but Clay and Grass, null included, maps to Hard, as the rating does.
    s3 = (
        pl.when(pl.col("surface").is_in(["Clay", "Grass"]))
        .then(pl.col("surface"))
        .otherwise(pl.lit("Hard"))
        .alias("s3")
    )
    ind = pl.col("indoor").fill_null(False).alias("ind")
    played = pl.col("svc_first_serve_pts_played") + pl.col(
        "svc_second_serve_pts_played"
    )
    won = pl.col("svc_first_serve_pts_won") + pl.col("svc_second_serve_pts_won")

    # 1-2. Contributions: completed in-scope rows with serve counts and an expectation.
    con = (
        df.lazy()
        .filter(
            scope
            & (played > 0)
            & won.is_not_null()
            & pl.col("player_bsr_pserve_logit").is_not_null()
        )
        .select(
            "tournament_id",
            "year",
            "circuit",
            "round_order",
            s3,
            ind,
            (pl.col("round_order") <= QUAL_MAX_ROUND).alias("qual"),
            played.alias("P"),
            (won - played / (1 + (-pl.col("player_bsr_pserve_logit")).exp())).alias(
                "R"
            ),
        )
        .group_by(_KEY + ["circuit", "year", "round_order", "qual"])
        .agg(pl.col("R").sum(), pl.col("P").sum())
        .collect()
    )
    # 3. Editions, pending ones included.
    eds = (
        df.lazy()
        .filter(scope)
        .select("tournament_id", "year", "circuit", s3, ind)
        .unique()
        .collect()
    )
    years = pl.concat([con.select("year"), eds.select("year")]).unique()

    # 4. Qualifying profile; every contribution is corrected with its own year's value.
    q = _qualifying_profile(con, years)
    con = (
        con.join(q, on=["circuit", "qual", "year"], how="left")
        .with_columns(pl.col("q").fill_null(0.0))
        .with_columns((pl.col("R") - pl.col("P") * pl.col("q")).alias("Rq"))
    )

    # 5. Cell means: raw residual rate over the three prior calendar years.
    cell_year = con.group_by(_CELL + ["year"]).agg(
        pl.col("R").sum().alias("cR"), pl.col("P").sum().alias("cP")
    )
    cell_mean = (
        eds.select(_CELL + ["year"])
        .unique()
        .join(cell_year.rename({"year": "py"}), on=_CELL)
        .filter(
            (pl.col("py") < pl.col("year"))
            & (pl.col("py") >= pl.col("year") - CELL_YEARS)
        )
        .group_by(_CELL + ["year"])
        .agg((pl.col("cR").sum() / pl.col("cP").sum()).alias("c"))
    )

    # 6. Earlier editions of the key, decayed.
    edition = con.group_by(_KEY + ["year"]).agg(
        pl.col("Rq").sum().alias("eR"), pl.col("P").sum().alias("eP")
    )
    weight = 0.5 ** ((pl.col("year") - pl.col("py")) / HALF_LIFE_YEARS)
    prior = (
        eds.select(_KEY + ["year"])
        .unique()
        .join(edition.rename({"year": "py"}), on=_KEY)
        .filter(pl.col("py") < pl.col("year"))
        .group_by(_KEY + ["year"])
        .agg(
            (pl.col("eR") * weight).sum().alias("Rp"),
            (pl.col("eP") * weight).sum().alias("Pp"),
        )
    )

    # 7. Lower rounds of the same edition, for every round 1..N_ROUNDS.
    rounds = pl.DataFrame({"r": list(range(1, N_ROUNDS + 1))}, schema={"r": pl.Int64})
    grid = eds.join(rounds, how="cross")
    by_round = con.group_by(_KEY + ["year", "round_order"]).agg(
        pl.col("Rq").sum().alias("rR"), pl.col("P").sum().alias("rP")
    )
    lower = (
        grid.select(_KEY + ["year", "r"])
        .unique()
        .join(by_round, on=_KEY + ["year"], how="left")
        .filter(pl.col("round_order") < pl.col("r"))
        .group_by(_KEY + ["year", "r"])
        .agg(pl.col("rR").sum().alias("Rl"), pl.col("rP").sum().alias("Pl"))
    )

    # 8. One row per (edition, round).
    offsets = pl.DataFrame(
        [
            {"s3": s, "ind": i, "off": _surface_offset(s, i)}
            for s in SURFACE_INDEX
            for i in (False, True)
        ],
        schema={"s3": pl.String, "ind": pl.Boolean, "off": pl.Float64},
    )
    table = (
        grid.join(cell_mean, on=_CELL + ["year"], how="left")
        .join(prior, on=_KEY + ["year"], how="left")
        .join(lower, on=_KEY + ["year", "r"], how="left")
        .join(offsets, on=["s3", "ind"], how="left")
        .with_columns([pl.col(c).fill_null(0.0) for c in ("c", "Rp", "Pp", "Rl", "Pl")])
        .with_columns(
            (
                100.0
                * (pl.col("Rp") + pl.col("Rl") + PRIOR_STRENGTH * pl.col("c"))
                / (pl.col("Pp") + pl.col("Pl") + PRIOR_STRENGTH)
            ).alias("court_pace"),
            (pl.col("Pp") + pl.col("Pl")).alias("court_pace_n"),
        )
        .with_columns((pl.col("court_pace") + pl.col("off")).alias("court_pace_full"))
        .select(_KEY + ["year", "circuit", "r"] + _OUTPUTS)
    )
    dupes = table.select(_KEY + ["year", "circuit", "r"]).is_duplicated().sum()
    if dupes:
        raise ValueError(
            f"court_pace_index: {dupes} duplicate (edition, round) rows "
            "in the pace table"
        )

    # 9. Onto the in-scope rows, then onto every row (out of scope stays null).
    rows = (
        df.lazy()
        .filter(scope)
        .select(
            "match_uid",
            "player_id",
            "tournament_id",
            "year",
            "circuit",
            "round_order",
            s3,
            ind,
        )
        .collect()
    )
    out = rows.join(
        table,
        left_on=_KEY + ["year", "circuit", "round_order"],
        right_on=_KEY + ["year", "circuit", "r"],
        how="left",
    ).select("match_uid", "player_id", *_OUTPUTS)
    return (
        df.select("match_uid", "player_id")
        .join(out, on=["match_uid", "player_id"], how="left")
        .select("match_uid", "player_id", *_OUTPUTS)
    )


register_transform(
    name="court_pace_index",
    func=_court_pace_transform,
    outputs=_OUTPUTS,
    raw_columns=_RAW,
    description=(
        "Court pace per tournament edition: serve points won above the serve-return "
        "rating's expectation, net of qualifying, from earlier editions (half-life "
        "1 year) and lower rounds of the same edition, shrunk to the cell mean"
    ),
)


def _court_pace_serve_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Engine transform: court pace times centred serve power, player/opp/diff."""
    n = df.select("match_uid", "player_id").is_duplicated().sum()
    if n:
        raise ValueError(f"court_pace_serve: {n} duplicate (match_uid, player_id) rows")
    pace = (
        _court_pace_transform(df)
        .select("match_uid", "player_id", *_SERVE_PACES)
        .filter(pl.col("court_pace_full").is_not_null())
    )
    # effective_match_date carries times of day on live rows; the field keys on
    # the calendar day so same-day rows never see each other.
    rows = df.select(
        "match_uid",
        "player_id",
        pl.col("effective_match_date").dt.date().alias("day"),
        *[p.cast(pl.Float64).alias(f"a_{k}") for k, (p, _) in _SERVE_ATTRS.items()],
        *[o.cast(pl.Float64).alias(f"o_{k}") for k, (_, o) in _SERVE_ATTRS.items()],
    ).join(pace, on=["match_uid", "player_id"], how="inner")

    # Field mean of each attribute over in-scope rows in [day - FIELD_DAYS, day).
    for k in _SERVE_ATTRS:
        daily = (
            rows.filter(pl.col(f"a_{k}").is_not_null())
            .group_by("day")
            .agg(
                pl.col(f"a_{k}").sum().alias("s"), pl.len().cast(pl.Float64).alias("n")
            )
            .sort("day")
            .with_columns(
                pl.col("s").rolling_sum_by(
                    "day", window_size=f"{FIELD_DAYS}d", closed="left"
                ),
                pl.col("n").rolling_sum_by(
                    "day", window_size=f"{FIELD_DAYS}d", closed="left"
                ),
            )
            .select("day", (pl.col("s") / pl.col("n")).alias(f"c_{k}"))
        )
        rows = rows.join(daily, on="day", how="left")

    for k in _SERVE_ATTRS:
        for p in _SERVE_PACES:
            rows = rows.with_columns(
                (pl.col(p) * (pl.col(f"a_{k}") - pl.col(f"c_{k}"))).alias(
                    f"player_{p}_{k}"
                ),
                (pl.col(p) * (pl.col(f"o_{k}") - pl.col(f"c_{k}"))).alias(
                    f"opp_{p}_{k}"
                ),
            ).with_columns(
                (pl.col(f"player_{p}_{k}") - pl.col(f"opp_{p}_{k}")).alias(
                    f"player_{p}_{k}_diff"
                )
            )
    return (
        df.select("match_uid", "player_id")
        .join(
            rows.select("match_uid", "player_id", *_SERVE_OUTPUTS),
            on=["match_uid", "player_id"],
            how="left",
        )
        .select("match_uid", "player_id", *_SERVE_OUTPUTS)
    )


register_transform(
    name="court_pace_serve",
    func=_court_pace_serve_transform,
    outputs=_SERVE_OUTPUTS,
    depends_on=["style_axis_serve", "style_avg_1st_serve_speed"],
    raw_columns=_SERVE_RAW,
    description=(
        "Court pace, within surface and surface included, times serve power "
        "(style serve axis, rating ace skill, rating serve-minus-return skill, "
        "first-serve speed), each centred on its prior 730-day field mean; "
        "player, opponent and diff"
    ),
)
