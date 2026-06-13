"""Cross-tournament aggregation into a single enriched matches dataset."""

import glob
import logging
import re
from pathlib import Path

import polars as pl

from mvp.atptour.ratings import compute_all_ratings
from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)

ROUND_ORDER: dict[str, int] = {
    "Q1": 1,
    "Q2": 2,
    "Q3": 3,
    "RR": 4,
    "R128": 5,
    "R64": 6,
    "R32": 7,
    "R16": 8,
    "QF": 9,
    "SF": 10,
    "THIRDPLACE": 11,
    "HCF": 11,
    "BRONZE": 11,
    "F": 12,
}

# Effective-span cap (days) for ITF/Challenger date estimation. Historical
# Activity data merges consecutive weeks into long spans, and the live end_date
# can run past a player's real last match; anchoring the round offsets to
# min(end, start + CAP) keeps a tournament's estimated rounds inside a realistic
# window so adjacent weeks can't overlap and interleave a player's two events.
# Tour events legitimately span 1-2 weeks and are NOT capped.
_ITF_CHAL_SPAN_CAP_DAYS = 6

# Draw-size-aware day-offsets from the (capped) tournament end, one profile per
# draw-size bucket. Each value is "days before the Final" for that round_order,
# from the empirical median (round -> days before the Final) over matches with a
# real scheduled_datetime. A 128-draw plays ~2 days/round, a 32-draw ~1. Each
# profile lists only the rounds its bucket usually has; _complete_offsets fills
# the rest monotonically. round_order keys: Q1=1 Q2=2 Q3=3 RR=4 R128=5 R64=6
# R32=7 R16=8 QF=9 SF=10 (playoff=11) F=12.
_DRAW_ROUND_OFFSETS: dict[str, dict[int, int]] = {
    "P32":  {1: 7,  2: 6,                       7: 5, 8: 3, 9: 2, 10: 1, 12: 0},
    "P64":  {1: 8,  2: 7,              6: 6,     7: 4, 8: 3, 9: 2, 10: 1, 12: 0},
    "P96":  {1: 13, 2: 12, 5: 10,      6: 8,     7: 6, 8: 5, 9: 3, 10: 2, 12: 0},
    "P128": {1: 19, 2: 18, 3: 17, 5: 13, 6: 11,  7: 9, 8: 7, 9: 5, 10: 2, 12: 0},
}

_ALL_ROUND_ORDERS = list(range(1, 13))


def _complete_offsets(listed: dict[int, int]) -> dict[int, int]:
    """Fill every round_order 1-12 with a monotonic non-increasing day-offset.

    Listed rounds keep their empirical offset; an unlisted round (e.g. Q3/RR in a
    small draw) is linearly interpolated between its nearest listed neighbours and
    then clamped so the offset never *increases* as round_order increases. Ties
    are fine — the ``[date, ..., round_order, ...]`` sort key keeps tied-date
    rounds in true round order. This replaces the old flat ``F - round_order``
    fallback, which handed unlisted early rounds a larger offset than the round
    before them and inverted them (Q3 landing before Q2 in P32/P64).
    """
    ros = sorted(listed)
    full: dict[int, int] = {}
    for ro in _ALL_ROUND_ORDERS:
        if ro in listed:
            full[ro] = listed[ro]
            continue
        lower = [r for r in ros if r < ro]
        higher = [r for r in ros if r > ro]
        if lower and higher:
            rl, rh = max(lower), min(higher)
            frac = (ro - rl) / (rh - rl)
            full[ro] = round(listed[rl] + frac * (listed[rh] - listed[rl]))
        elif higher:  # earlier than any listed round: step up from the first
            rh = min(higher)
            full[ro] = listed[rh] + (rh - ro)
        else:  # later than the last listed round (F=12 is always listed: unused)
            full[ro] = listed[max(lower)]
    # Enforce monotonic non-increasing offset as round_order rises.
    prev = None
    for ro in _ALL_ROUND_ORDERS:
        if prev is not None and full[ro] > prev:
            full[ro] = prev
        prev = full[ro]
    return full


_DRAW_FULL_OFFSETS: dict[str, dict[int, int]] = {
    bucket: _complete_offsets(prof) for bucket, prof in _DRAW_ROUND_OFFSETS.items()
}
# Guard: no profile may increase in offset as round_order rises (i.e. no round
# may be dated before an earlier round of the same tournament).
for _bucket, _prof in _DRAW_FULL_OFFSETS.items():
    _seq = [_prof[r] for r in _ALL_ROUND_ORDERS]
    assert all(a >= b for a, b in zip(_seq, _seq[1:])), f"non-monotonic offsets: {_bucket}"

# Flattened {"<bucket>_<round_order>": offset} for a single replace_strict lookup.
_DRAW_OFFSET_MAP: dict[str, int] = {
    f"{bucket}_{ro}": off
    for bucket, prof in _DRAW_FULL_OFFSETS.items()
    for ro, off in prof.items()
}

# Main-draw rounds in elimination order, mapped to depth from a 128 draw.
# round_order has a gap at 11 (playoff rounds) so depth != round_order; this
# map is what makes the tournament round ordinal evenly spaced.
_MAIN_DRAW_DEPTH: dict[int, int] = {5: 0, 6: 1, 7: 2, 8: 3, 9: 4, 10: 5, 12: 6}
# Singles draw size -> opening round_order, for standard brackets. 96 and any
# non-standard size are omitted and fall through to the rounds-derived opener.
_DRAW_SIZE_OPENER: dict[int, int] = {28: 7, 32: 7, 48: 6, 56: 6, 64: 6, 128: 5}


def filter_dc_tournaments(df: pl.DataFrame) -> pl.DataFrame:
    """Exclude Davis Cup and team events from tournament matches."""
    return df.filter(
        ~(
            pl.col("event_type").str.starts_with("DC").fill_null(False)
            | (pl.col("circuit") == "team")
        )
    )


def filter_dc_activity(df: pl.DataFrame) -> pl.DataFrame:
    """Exclude Davis Cup and team event rows from Activity data."""
    return df.filter(
        (pl.col("event_type") != "DC") & (pl.col("circuit") != "team")
    )


def map_activity_to_matches_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Map Activity columns to matches schema for gap-fill rows.

    Renames rank fields, derives won from win_loss, adds draw_type.
    Does NOT add null columns for stats/overview -- that happens during concat.
    """
    return df.select(
        "match_uid",
        "player_id",
        "opp_id",
        "tournament_id",
        "tournament_name",
        "year",
        "circuit",
        pl.lit("singles").alias("draw_type"),
        "round",
        "surface",
        "indoor",
        "event_type",
        "tournament_start_date",
        "tournament_end_date",
        (pl.col("win_loss") == "W").alias("won"),
        "reason",
        pl.col("player_rank").alias("activity_rank"),
        pl.col("opp_rank").alias("activity_opp_rank"),
        pl.col("points").alias("activity_points"),
        *[f"player_set{n}_{k}" for n in range(1, 6) for k in ("games", "tiebreak")],
        *[f"opp_set{n}_{k}" for n in range(1, 6) for k in ("games", "tiebreak")],
    )


def join_rankings(matches: pl.DataFrame, rankings: pl.DataFrame) -> pl.DataFrame:
    """Join rankings data for both player and opponent using as-of join.

    For each match, finds the most recent rankings snapshot on or before
    tournament_start_date for both player_id and opp_id.
    """
    rnk = rankings.select([
        "player_id", "ranking_date", "rank", "points", "tournaments_played",
    ]).sort("ranking_date")

    # Player rankings
    player_rnk = rnk.rename({
        "player_id": "_rnk_pid",
        "rank": "player_rankings_rank",
        "points": "player_rankings_points",
        "tournaments_played": "player_rankings_tournaments_played",
    })
    result = matches.sort("tournament_start_date").join_asof(
        player_rnk,
        left_on="tournament_start_date",
        right_on="ranking_date",
        by_left="player_id",
        by_right="_rnk_pid",
        strategy="backward",
    )

    # Opponent rankings
    opp_rnk = rnk.rename({
        "player_id": "_rnk_pid",
        "rank": "opp_rankings_rank",
        "points": "opp_rankings_points",
        "tournaments_played": "opp_rankings_tournaments_played",
    })
    result = result.sort("tournament_start_date").join_asof(
        opp_rnk,
        left_on="tournament_start_date",
        right_on="ranking_date",
        by_left="opp_id",
        by_right="_rnk_pid",
        strategy="backward",
    )

    # Drop temporary ranking_date columns from both join_asof calls
    drop_cols = [c for c in result.columns if c.startswith("ranking_date")]
    return result.drop(drop_cols)


_BIO_FIELDS = [
    "first_name", "last_name", "height_cm", "weight_kg",
    "right_handed", "twohand_backhand", "birth_date", "pro_year",
    "nationality", "natl_id",
]


def join_player_bio(matches: pl.DataFrame, bio: pl.DataFrame) -> pl.DataFrame:
    """Join PlayerBio data for both player and opponent.

    Adds player_first_name, player_last_name, ..., opp_first_name, etc.
    """
    bio_subset = bio.select(["player_id"] + _BIO_FIELDS)

    # Player bio
    player_bio = bio_subset.rename(
        {f: f"player_{f}" for f in _BIO_FIELDS} | {"player_id": "_bio_pid"}
    )
    result = matches.join(
        player_bio, left_on="player_id", right_on="_bio_pid", how="left"
    )

    # Opponent bio
    opp_bio = bio_subset.rename(
        {f: f"opp_{f}" for f in _BIO_FIELDS} | {"player_id": "_bio_pid"}
    )
    result = result.join(opp_bio, left_on="opp_id", right_on="_bio_pid", how="left")

    return result


def fill_tournament_dates(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize tournament_start_date and tournament_end_date within each tournament.

    Every row in a (tournament_id, year) takes the group's max date. This both
    fills nulls AND collapses conflicting non-null dates to a single value, so a
    tournament can never carry two start/end dates. The round-anchored date
    estimator floors rounds at tournament_start_date and the rating/feature sort
    key tiebreaks on it — both require one value per tournament, or a tournament
    with two listed starts inverts its own rounds (e.g. Q1 floored a day after
    Q2). Only tournaments with ALL nulls remain null.
    """
    group_keys = ["tournament_id", "year"]
    return df.with_columns([
        pl.col("tournament_start_date").max().over(group_keys).alias("tournament_start_date"),
        pl.col("tournament_end_date").max().over(group_keys).alias("tournament_end_date"),
    ])


def fill_tournament_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Fill tournament-level fields within each tournament group.

    For surface, indoor, event_type: if any row in a tournament has a value,
    propagate it to all rows. Only tournaments with ALL nulls remain null.
    """
    group_keys = ["tournament_id", "year"]
    fill_cols = ["surface", "indoor", "event_type", "tournament_name", "country",
                  "sponsor_title"]
    return df.with_columns([
        pl.col(c)
        .fill_null(pl.col(c).drop_nulls().first().over(group_keys))
        .alias(c)
        for c in fill_cols
        if c in df.columns
    ])


def _strip_trailing_number(name: str) -> str:
    """Strip trailing ' N' suffix from a tournament name."""
    return re.sub(r"\s+\d+$", "", name)


def disambiguate_tournament_names(df: pl.DataFrame) -> pl.DataFrame:
    """Append trailing number from sponsor_title to tournament_name for same-name collisions.

    When multiple tournament_ids share the same tournament_name in a year,
    check sponsor_title for a trailing integer (e.g. "Rwanda Challenger 2" -> 2)
    and append it to tournament_name (e.g. "Kigali" -> "Kigali 2").
    When no sponsor_title has a trailing number, assigns sequential numbers
    by sorted tournament_id.
    """
    if "sponsor_title" not in df.columns or "tournament_name" not in df.columns:
        return df

    # Strip any existing trailing number suffixes to get base names,
    # preventing double-suffixing (e.g. "Hersonissos 1" -> "Hersonissos 1 1")
    df = df.with_columns(
        pl.col("tournament_name")
        .map_elements(_strip_trailing_number, return_dtype=pl.Utf8)
        .alias("tournament_name")
    )

    # Find (tournament_name, year) pairs with multiple tournament_ids
    collision_keys = (
        df.select("tournament_id", "year", "tournament_name")
        .drop_nulls()
        .unique()
        .group_by("tournament_name", "year")
        .agg(pl.col("tournament_id").n_unique().alias("n_ids"))
        .filter(pl.col("n_ids") > 1)
        .select("tournament_name", "year")
    )

    if collision_keys.is_empty():
        return df

    # For colliding tournaments, build a tid -> suffix mapping from sponsor_title
    colliding = (
        df.join(collision_keys, on=["tournament_name", "year"], how="semi")
        .select("tournament_id", "year", "sponsor_title")
        .drop_nulls()
        .unique(subset=["tournament_id", "year"])
    )

    suffix_map: dict[tuple[str, int], str] = {}
    for row in colliding.iter_rows(named=True):
        title = row["sponsor_title"]
        m = re.search(r"\s+(\d+)\s*$", title)
        if m:
            suffix_map[(row["tournament_id"], row["year"])] = m.group(1)

    # Build collision groups: (tournament_name, year) -> sorted list of tids
    collision_tids = (
        df.join(collision_keys, on=["tournament_name", "year"], how="semi")
        .select("tournament_id", "year", "tournament_name")
        .drop_nulls()
        .unique()
        .sort("tournament_id")
    )
    groups: dict[tuple[str, int], list[str]] = {}
    for row in collision_tids.iter_rows(named=True):
        groups.setdefault((row["tournament_name"], row["year"]), []).append(
            row["tournament_id"]
        )

    # Assign numbers to unsuffixed tournaments
    for (_tname, yr), tids in groups.items():
        unsuffixed = [tid for tid in tids if (tid, yr) not in suffix_map]
        if not unsuffixed:
            continue
        if any((tid, yr) in suffix_map for tid in tids):
            # Some have sponsor_title numbers — unsuffixed ones get "1"
            for tid in unsuffixed:
                suffix_map[(tid, yr)] = "1"
        else:
            # No sponsor_title numbers at all — assign by sorted tournament_id
            for i, tid in enumerate(tids, start=1):
                suffix_map[(tid, yr)] = str(i)

    if not suffix_map:
        return df

    # Build a small lookup frame and join
    suffix_df = pl.DataFrame([
        {"tournament_id": tid, "year": yr, "_name_suffix": f" {suffix}"}
        for (tid, yr), suffix in suffix_map.items()
    ])

    df = df.join(suffix_df, on=["tournament_id", "year"], how="left")
    df = df.with_columns(
        pl.when(pl.col("_name_suffix").is_not_null())
        .then(pl.col("tournament_name") + pl.col("_name_suffix"))
        .otherwise(pl.col("tournament_name"))
        .alias("tournament_name")
    ).drop("_name_suffix")

    renamed = len(suffix_map)
    logger.info("Tournament name disambiguation: %d tournament-years renamed", renamed)
    return df


def add_round_order(df: pl.DataFrame) -> pl.DataFrame:
    """Add round_order column from the round column using ROUND_ORDER mapping."""
    return df.with_columns(
        pl.col("round")
        .replace_strict(ROUND_ORDER, default=None)
        .cast(pl.Int64)
        .alias("round_order")
    )


def add_draw_round_ordinal(df: pl.DataFrame) -> pl.DataFrame:
    """Add `tournament_round_ordinal`: the round's signed position within its draw.

    Main-draw rounds count up from the opener (opener = +1, next = +2, ... final);
    qualifying rounds count down toward the main draw (last qualifier = -1, ...).
    So R32 is +1 in a 32-draw (it IS the opener) but +2 in a 64-draw (round 2) —
    a distinction the global `round_order` cannot express.

    The opener is resolved from `singles_draw_size` where available (robust to the
    occasional mislabeled round), falling back to the earliest main-draw round
    observed in the (tournament_id, year, draw_type) edition otherwise (ITF, 96-
    draws, missing draw size). The draw is published weeks before play, so this
    value is known well pre-match. Non-standard rounds (RR, playoff) get null.

    Depends on `round_order` — call after `add_round_order`.
    """
    grp = ["tournament_id", "year", "draw_type"]
    ro = pl.col("round_order")
    is_main = ro.is_in(list(_MAIN_DRAW_DEPTH.keys()))
    is_qual = ro.is_in([1, 2, 3])

    # Opener round_order: draw-size lookup (singles) first, else earliest observed
    # main-draw round in the edition.
    lookup = (
        pl.when(pl.col("draw_type") == "singles")
        .then(
            pl.col("singles_draw_size")
            .cast(pl.Int64, strict=False)
            .replace_strict(_DRAW_SIZE_OPENER, default=None, return_dtype=pl.Int64)
        )
        .otherwise(None)
    )
    derived = pl.when(is_main).then(ro).otherwise(None).min().over(grp)
    opener_ro = pl.coalesce([lookup, derived])

    depth = ro.replace_strict(_MAIN_DRAW_DEPTH, default=None, return_dtype=pl.Int64)
    opener_depth = opener_ro.replace_strict(
        _MAIN_DRAW_DEPTH, default=None, return_dtype=pl.Int64
    )
    max_qual = pl.when(is_qual).then(ro).otherwise(None).max().over(grp)

    ordinal = (
        pl.when(is_main)
        .then(depth - opener_depth + 1)
        .when(is_qual)
        .then(ro - max_qual - 1)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("tournament_round_ordinal")
    )
    return df.with_columns(ordinal)


def add_effective_match_date(df: pl.DataFrame) -> pl.DataFrame:
    """Add effective_match_date column using schedule data or round-offset estimation.

    For each (tournament_id, year) group:
    - If ALL scheduled_datetime values are non-null, use scheduled_datetime directly.
    - Otherwise, estimate by scaling round position across the tournament duration.
      Early rounds (Q1, R128) are placed near tournament_start_date, late rounds (F)
      near tournament_end_date. This handles both short Challengers and 2-week Slams.

    Any row with a null scheduled_datetime (a walkover that was never played, or
    a match whose schedule the feed dropped) is dated from the max
    scheduled_datetime of peer matches in the same
    (tournament_id, year, draw_type, round). Anchoring to round peers keeps the
    fill inside the round's real window, so it sorts alongside its round and can
    never slot after the winner's next-round match. This also means a single
    missing schedule no longer poisons the group-level passthrough gate and
    forces the whole tournament into round-offset estimation. A round where
    *every* match is null has no peer to borrow from and stays null, so a
    genuinely unscheduled tournament still falls through to estimation below.
    """
    group_keys = ["tournament_id", "year"]

    # Resolved schedule: fill any null scheduled_datetime from round peers (see
    # docstring). result_type is no longer needed — walkovers are just one source
    # of nulls and are covered by the same peer fill.
    peer_sched = pl.col("scheduled_datetime").max().over(
        ["tournament_id", "year", "draw_type", "round"]
    )
    df = df.with_columns(
        pl.when(pl.col("scheduled_datetime").is_null())
        .then(peer_sched)
        .otherwise(pl.col("scheduled_datetime"))
        .alias("_resolved_sched"),
    )

    # Per-group flag: True if every row in the group has a non-null resolved sched
    df = df.with_columns(
        pl.col("_resolved_sched")
        .is_not_null()
        .all()
        .over(group_keys)
        .alias("_all_scheduled"),
    )

    # Estimated dates: anchor the Final to the (capped) tournament end and step
    # back a draw-size-aware number of days per round (_DRAW_FULL_OFFSETS). Within
    # a tournament offsets are monotonic non-increasing in round_order, so rounds
    # never invert; rounds that share a day (compression near a short event's
    # start) stay correctly ordered via the [date, ..., round_order, match_uid]
    # sort key in the rating chain and cumulative features. The estimate depends
    # only on (tournament_start/end_date, round_order, draw size), so an
    # in-progress draw doesn't re-spread as it fills in.
    #
    # Every round — qualifying included — is floored at tournament_start so no
    # round can be dated before its own tournament's start and bleed into the
    # prior week's event. (Old code exempted qualifying, which let a large draw's
    # Q1 land ~5 days pre-start and interleave with the player's previous event.)
    # The ITF/Challenger span cap keeps the anchor from running into the next week.
    ro = pl.col("round_order").cast(pl.Int64)
    draw_size = (
        pl.col("singles_draw_size")
        if "singles_draw_size" in df.columns
        else pl.lit(None, dtype=pl.Int64)
    )
    # First main-draw round per tournament, to bucket events whose draw size is
    # null (R128 -> 96/128-style, R64 -> 64-style, R32 or smaller -> 32-style).
    first_main_ro = ro.filter(ro > 4).min().over(group_keys)
    draw_bucket = (
        pl.when(draw_size.is_not_null() & (draw_size <= 34)).then(pl.lit("P32"))
        .when(draw_size.is_not_null() & (draw_size <= 72)).then(pl.lit("P64"))
        .when(draw_size.is_not_null() & (draw_size <= 112)).then(pl.lit("P96"))
        .when(draw_size.is_not_null()).then(pl.lit("P128"))
        .when(first_main_ro == 5).then(pl.lit("P96"))
        .when(first_main_ro == 6).then(pl.lit("P64"))
        .otherwise(pl.lit("P32"))
    )
    offset = pl.coalesce(
        (draw_bucket + "_" + ro.cast(pl.String)).replace_strict(
            _DRAW_OFFSET_MAP, default=None, return_dtype=pl.Int64
        ),
        ROUND_ORDER["F"] - ro,  # safety for any round_order outside 1-12
    )
    start_dt = pl.col("tournament_start_date").cast(pl.Datetime)
    end_dt = pl.col("tournament_end_date").cast(pl.Datetime)
    # Cap the anchor for ITF/Challenger so a merged/overlong source window (or a
    # live end_date past the player's real last match) can't spread the rounds
    # into the following week. Tour events span 1-2 weeks legitimately: not capped.
    if "circuit" in df.columns:
        capped_end = (
            pl.when(pl.col("circuit").is_in(["itf", "chal"]))
            .then(pl.min_horizontal(
                end_dt, start_dt + pl.duration(days=_ITF_CHAL_SPAN_CAP_DAYS)
            ))
            .otherwise(end_dt)
        )
    else:
        capped_end = end_dt
    raw_est = capped_end - pl.duration(days=offset)
    estimated = (
        pl.when(ro.is_null())
        # Unknown/unmapped round: can't place it by round, fall back to start.
        .then(start_dt)
        # All rounds (qualifying included) floored at the listed start.
        .otherwise(pl.max_horizontal(start_dt, raw_est))
    )

    df = df.with_columns(
        pl.when(pl.col("_all_scheduled"))
        .then(pl.col("_resolved_sched"))
        .otherwise(estimated)
        .alias("effective_match_date"),
    )

    # Validate no nulls
    bad_rows = df.filter(pl.col("effective_match_date").is_null())
    null_count = bad_rows.height
    if null_count > 0:
        # Summarize by tournament/year for actionable diagnosis
        summary = (
            bad_rows
            .group_by(["tournament_id", "year", "circuit"])
            .agg([
                pl.len().alias("count"),
                pl.col("tournament_start_date").is_null().sum().alias("null_start_date"),
                pl.col("round_order").is_null().sum().alias("null_round_order"),
            ])
            .sort(["year", "tournament_id"])
        )
        logger.error(
            "Missing effective_match_date: %d rows across %d tournaments",
            null_count,
            summary.height,
        )
        logger.error("Affected tournaments (tournament_id, year, circuit, count, "
                     "null_start_date, null_round_order):")
        for row in summary.iter_rows(named=True):
            logger.error(
                "  %s/%s (%s): %d matches, %d missing start_date, %d missing round_order",
                row["tournament_id"],
                row["year"],
                row["circuit"],
                row["count"],
                row["null_start_date"],
                row["null_round_order"],
            )

        # Log sample of actual bad rows (limited to 20)
        detail_cols = group_keys + [
            "match_uid", "round", "round_order",
            "tournament_start_date", "scheduled_datetime",
        ]
        sample_rows = bad_rows.select(detail_cols).head(20)
        logger.debug("Sample rows with null effective_match_date:\n%s", sample_rows)

        msg = (
            f"null effective_match_date: {null_count} rows "
            f"across {summary.height} tournaments"
        )
        raise ValueError(msg)

    # Drop temporary columns
    temp_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(temp_cols)


def add_tournament_level(df: pl.DataFrame) -> pl.DataFrame:
    """Derive tournament_level from event_type + event_type_detail."""
    has_detail = "event_type_detail" in df.columns
    if has_detail:
        return df.with_columns(
            pl.when(pl.col("event_type") == "CH")
            .then(
                pl.when(pl.col("event_type_detail") == 175).then(pl.lit("CH175"))
                .when(pl.col("event_type_detail") == 125).then(pl.lit("CH125"))
                .when(pl.col("event_type_detail") == 110).then(pl.lit("CH100"))
                .when(pl.col("event_type_detail") == 100).then(pl.lit("CH100"))
                .when(pl.col("event_type_detail") == 90).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 80).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 75).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 50).then(pl.lit("CH50"))
                .otherwise(pl.lit("CH75"))
            )
            .when(pl.col("event_type") == "FU").then(pl.lit("FU"))
            .when(pl.col("event_type") == "GS").then(pl.lit("GS"))
            .when(pl.col("event_type") == "1000").then(pl.lit("1000"))
            .when(pl.col("event_type").is_in(["CS", "WC"])).then(pl.lit("1000"))
            .when(pl.col("event_type") == "500").then(pl.lit("500"))
            .when(pl.col("event_type") == "GP").then(pl.lit("500"))
            .when(pl.col("event_type") == "OL").then(pl.lit("500"))
            .when(pl.col("event_type") == "250").then(pl.lit("250"))
            .otherwise(pl.lit("250"))
            .alias("tournament_level")
        )
    # No detail column — map from event_type alone
    return df.with_columns(
        pl.when(pl.col("event_type") == "CH").then(pl.lit("CH75"))
        .when(pl.col("event_type") == "FU").then(pl.lit("FU"))
        .when(pl.col("event_type") == "GS").then(pl.lit("GS"))
        .when(pl.col("event_type") == "1000").then(pl.lit("1000"))
        .when(pl.col("event_type").is_in(["CS", "WC"])).then(pl.lit("1000"))
        .when(pl.col("event_type") == "500").then(pl.lit("500"))
        .when(pl.col("event_type") == "GP").then(pl.lit("500"))
        .when(pl.col("event_type") == "OL").then(pl.lit("500"))
        .when(pl.col("event_type") == "250").then(pl.lit("250"))
        .otherwise(pl.lit("250"))
        .alias("tournament_level")
    )


def add_best_of(df: pl.DataFrame) -> pl.DataFrame:
    """Derive best_of (3 or 5) from tournament metadata.

    Rules:
      - Grand Slam main draw (non-qualifying) → 5
      - Wimbledon Q3 (final qualifying round) → 5
      - Jeddah / Six Kings (tournament_id 7696) → 5
      - Everything else → 3
    """
    is_gs_main = (pl.col("tournament_level") == "GS") & pl.col("round").is_in(
        ["R128", "R64", "R32", "R16", "QF", "SF", "F", "RR"]
    )
    is_wimbledon_q3 = (
        (pl.col("tournament_name") == "Wimbledon") & (pl.col("round") == "Q3")
    )
    is_jeddah = pl.col("tournament_id") == "7696"

    return df.with_columns(
        pl.when(is_gs_main | is_wimbledon_q3 | is_jeddah)
        .then(pl.lit(5))
        .otherwise(pl.lit(3))
        .alias("best_of")
    )


def add_partner_workload_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Add rows for doubles partners so workload features count their appearances.

    For each doubles match row where player_partner_id is not null, creates
    a corresponding row with player_id = player_partner_id. These rows have
    null stats (since individual stats aren't available for partners) but
    preserve the match metadata for workload counting.

    This ensures matches_played(days=30) counts doubles appearances for
    players who appear as partners, not just as the primary player_id.
    """
    # Get doubles rows with partners
    doubles_with_partner = df.filter(
        (pl.col("draw_type") == "doubles")
        & pl.col("player_partner_id").is_not_null()
    )

    if doubles_with_partner.is_empty():
        return df

    # Columns to keep from original (match metadata, no stats)
    metadata_cols = [
        "match_uid", "tournament_id", "year", "circuit", "draw_type",
        "round", "round_order", "surface", "indoor", "event_type",
        "tournament_name", "country",
        "tournament_start_date", "tournament_end_date",
        "effective_match_date", "won",
    ]

    # Keep only columns that exist
    keep_cols = [c for c in metadata_cols if c in df.columns]

    # Create partner rows: swap player_id with player_partner_id
    partner_rows = doubles_with_partner.select(
        keep_cols + ["player_partner_id", "player_id", "opp_partner_id", "opp_id"]
    ).rename({
        "player_partner_id": "player_id",
        "player_id": "player_partner_id",
        "opp_partner_id": "opp_id",
        "opp_id": "opp_partner_id",
    })

    # Concat with diagonal_relaxed to handle missing columns (they become null)
    return pl.concat([df, partner_rows], how="diagonal_relaxed")


def validate_tournament_scheduling(df: pl.DataFrame) -> list[dict]:
    """Flag players with impossible scheduling patterns.

    Detects:
    1. Same effective_match_date, different tournaments
    2. Interleaved tournaments (A, B, A pattern within a short window)

    Returns a list of warning dicts describing the conflicts.
    """
    warnings: list[dict] = []

    # Get player-match-date-tournament combinations
    matches = (
        df.filter(pl.col("effective_match_date").is_not_null())
        .select(["player_id", "tournament_id", "effective_match_date"])
        .unique()
        .sort(["player_id", "effective_match_date"])
    )

    # Check for invalid dates (year < 1 causes Python conversion errors)
    bad_dates = df.filter(pl.col("effective_match_date").dt.year() < 1)
    if bad_dates.height > 0:
        logger.error(
            "Found %d rows with invalid effective_match_date (year < 1):",
            bad_dates.height,
        )
        # Use Polars to extract without Python date conversion
        sample = bad_dates.head(20).with_columns(
            pl.col("effective_match_date").cast(pl.Utf8).alias("date_str")
        )
        for pid, tid, yr, date_str in zip(
            sample["player_id"].to_list(),
            sample["tournament_id"].to_list(),
            sample["year"].to_list(),
            sample["date_str"].to_list(),
        ):
            logger.error(
                "  player=%s, tournament=%s, year=%s, date=%s", pid, tid, yr, date_str
            )
        return warnings  # Skip validation, can't process bad dates

    # Check 1: Same day, different tournaments
    same_day = (
        matches.group_by(["player_id", "effective_match_date"])
        .agg(pl.col("tournament_id").n_unique().alias("n_tournaments"))
        .filter(pl.col("n_tournaments") > 1)
    )
    for row in same_day.iter_rows(named=True):
        tids = (
            matches.filter(
                (pl.col("player_id") == row["player_id"])
                & (pl.col("effective_match_date") == row["effective_match_date"])
            )["tournament_id"]
            .unique()
            .to_list()
        )
        warnings.append({
            "type": "same_day",
            "player_id": row["player_id"],
            "date": row["effective_match_date"],
            "tournament_ids": tids,
        })

    # Check 2: Interleaved tournaments (A on day N, B on day N+1, A on day N+2)
    for pid in matches["player_id"].unique().to_list():
        player_matches = (
            matches.filter(pl.col("player_id") == pid)
            .sort("effective_match_date")
            .select(["tournament_id", "effective_match_date"])
            .rows()
        )
        if len(player_matches) < 3:
            continue

        for i in range(len(player_matches) - 2):
            tid_a, date_a = player_matches[i]
            tid_b, date_b = player_matches[i + 1]
            tid_c, date_c = player_matches[i + 2]

            # A, B, A pattern within 7 days
            if tid_a == tid_c and tid_a != tid_b and (date_c - date_a).days <= 7:
                warnings.append({
                    "type": "interleaved",
                    "player_id": pid,
                    "pattern": [(tid_a, date_a), (tid_b, date_b), (tid_c, date_c)],
                })
                break  # One warning per player

    return warnings


def _downcast_int64(df: pl.DataFrame) -> pl.DataFrame:
    """Downcast Int64 columns to Int32 to reduce memory footprint."""
    i64_cols = [c for c in df.columns if df[c].dtype == pl.Int64]
    if not i64_cols:
        return df
    return df.with_columns(pl.col(c).cast(pl.Int32) for c in i64_cols)


class MatchesAggregator(BaseJob):
    """Cross-tournament aggregation into a single enriched matches dataset."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def aggregate(self) -> pl.DataFrame:
        """Run the full aggregation pipeline."""
        tournament_matches = _downcast_int64(self._stack_tournament_matches())
        logger.info("Tournament matches stacked: %d rows", len(tournament_matches))

        activity = _downcast_int64(self._load_activity())
        logger.info("Activity loaded: %d rows", len(activity))

        tournament_matches = self._enrich_from_activity(tournament_matches, activity)

        gap_fill = _downcast_int64(self._activity_gap_fill(tournament_matches, activity))
        logger.info("Activity gap-fill: %d rows", len(gap_fill))

        combined = pl.concat([tournament_matches, gap_fill], how="diagonal_relaxed")
        del tournament_matches, gap_fill, activity
        logger.info("Combined: %d rows", len(combined))

        # Step 6: Rankings enrichment
        rankings = self._load_rankings()
        if rankings is not None:
            combined = join_rankings(combined, rankings)
            del rankings
            logger.info("Rankings joined")

        # Step 7: Bio enrichment
        bio = self._load_bio()
        if bio is not None:
            combined = join_player_bio(combined, bio)
            del bio
            logger.info("Bio joined")

        # Coalesce rank columns: rankings (weekly snapshot) preferred, activity as fallback
        combined = combined.with_columns([
            pl.coalesce(["player_rankings_rank", "activity_rank"]).alias("player_rank"),
            pl.coalesce(["opp_rankings_rank", "activity_opp_rank"]).alias("opp_rank"),
        ]).drop(["player_rankings_rank", "opp_rankings_rank",
                 "activity_rank", "activity_opp_rank", "activity_points"])
        logger.info("Rank columns coalesced")

        # Step 8: Fill tournament-level fields within each tournament, then compute
        # effective match date. Any row in a tournament can provide the values.
        combined = fill_tournament_dates(combined)
        combined = fill_tournament_fields(combined)
        combined = disambiguate_tournament_names(combined)
        combined = add_round_order(combined)
        combined = add_draw_round_ordinal(combined)
        combined = add_effective_match_date(combined)
        combined = add_tournament_level(combined)
        combined = add_best_of(combined)

        # Compute Elo ratings for singles matches only
        # Pass only the columns ratings needs to avoid .to_dicts() on the full wide DF
        _RATINGS_INPUT_COLS = [
            "match_uid", "player_id", "opp_id", "effective_match_date", "round_order",
            "tournament_start_date",
            "surface", "round", "tournament_level", "won", "indoor",
            "player_rank", "opp_rank",
            "pts_service_pts_won", "pts_service_pts_played",
            "opp_pts_service_pts_won", "opp_pts_service_pts_played",
            "svc_aces", "svc_first_serve_pts_won",
            "svc_double_faults", "svc_second_serve_pts_played",
            "opp_svc_aces", "ret_first_serve_pts_played", "ret_first_serve_pts_won",
            "svc_bp_saved", "svc_bp_faced",
            "ret_bp_converted", "ret_bp_opportunities",
            "opp_svc_first_serve_pts_won",
            "opp_svc_double_faults", "opp_svc_second_serve_pts_played",
            "opp_ret_first_serve_pts_played", "opp_ret_first_serve_pts_won",
            "opp_svc_bp_saved", "opp_svc_bp_faced",
            "opp_ret_bp_converted", "opp_ret_bp_opportunities",
        ] + [f"player_set{i}_tiebreak" for i in range(1, 6)] + [
            f"opp_set{i}_tiebreak" for i in range(1, 6)
        ]
        _ratings_cols = [c for c in _RATINGS_INPUT_COLS if c in combined.columns]
        singles_slim = combined.filter(
            pl.col("draw_type") == "singles"
        ).select(_ratings_cols)
        if not singles_slim.is_empty():
            ratings_result = compute_all_ratings(singles_slim)
            # Extract only the new rating columns and join back
            join_keys = ["match_uid", "player_id"]
            rating_cols = [c for c in ratings_result.columns if c not in _ratings_cols]
            if rating_cols:
                combined = combined.join(
                    ratings_result.select(join_keys + rating_cols),
                    on=join_keys,
                    how="left",
                )
            del singles_slim, ratings_result

        # Step 9: Add partner rows for doubles workload tracking
        combined = add_partner_workload_rows(combined)
        logger.info("After partner expansion: %d rows", len(combined))

        combined = combined.sort(
            ["effective_match_date", "draw_type", "match_uid", "player_id"],
            nulls_last=True,
        )

        # Step 10: Validation
        warnings = validate_tournament_scheduling(combined)
        for w in warnings:
            if w["type"] == "same_day":
                logger.warning(
                    "Impossible scheduling: player %s in %d tournaments on %s: %s",
                    w["player_id"],
                    len(w["tournament_ids"]),
                    w["date"],
                    w["tournament_ids"],
                )
            elif w["type"] == "interleaved":
                logger.warning(
                    "Interleaved tournaments for player %s: %s",
                    w["player_id"],
                    w["pattern"],
                )

        return combined

    def _stack_tournament_matches(self) -> pl.DataFrame:
        """Glob and concat all per-tournament matches parquets, filtering DC."""
        pattern = str(
            self.data_root
            / "aggregate"
            / "atptour"
            / "tournaments"
            / "**"
            / "matches.parquet"
        )
        files = glob.glob(pattern, recursive=True)
        if not files:
            return pl.DataFrame()
        dfs = [pl.read_parquet(f) for f in files]
        stacked = pl.concat(dfs, how="diagonal_relaxed")
        return filter_dc_tournaments(stacked)

    def _load_activity(self) -> pl.DataFrame:
        """Load Activity parquet and filter out DC and byes."""
        path = self.data_root / "stage" / "atptour" / "activity.parquet"
        if not path.exists():
            return pl.DataFrame()
        act = pl.read_parquet(path)
        act = filter_dc_activity(act)
        act = act.filter(
            (pl.col("is_bye") == False) & pl.col("match_uid").is_not_null()  # noqa: E712
        )
        return act

    def _enrich_from_activity(
        self, matches: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Activity fields onto overlapping tournament matches.

        Enriches rank fields AND tournament-level fields (surface, indoor, etc.)
        that may be missing from per-tournament matches but present in activity.
        """
        if activity.is_empty():
            return matches.with_columns([
                pl.lit(None).cast(pl.Int64).alias("activity_rank"),
                pl.lit(None).cast(pl.Int64).alias("activity_opp_rank"),
                pl.lit(None).cast(pl.Int64).alias("activity_points"),
            ])
        act_enrichment = activity.select([
            "match_uid",
            "player_id",
            pl.col("player_rank").alias("activity_rank"),
            pl.col("opp_rank").alias("activity_opp_rank"),
            pl.col("points").alias("activity_points"),
            pl.col("tournament_start_date").alias("_act_start_date"),
            pl.col("tournament_end_date").alias("_act_end_date"),
            pl.col("surface").alias("_act_surface"),
            pl.col("indoor").alias("_act_indoor"),
            pl.col("event_type").alias("_act_event_type"),
        ])

        result = matches.join(
            act_enrichment, on=["match_uid", "player_id"], how="left"
        )

        # Fill fields from Activity where tournament matches are missing them
        if "_act_start_date" in result.columns:
            result = result.with_columns([
                pl.coalesce([
                    pl.col("tournament_start_date"),
                    pl.col("_act_start_date"),
                ]).alias("tournament_start_date"),
                pl.coalesce([
                    pl.col("tournament_end_date"),
                    pl.col("_act_end_date"),
                ]).alias("tournament_end_date"),
                pl.coalesce([
                    pl.col("surface"),
                    pl.col("_act_surface"),
                ]).alias("surface"),
                pl.coalesce([
                    pl.col("indoor"),
                    pl.col("_act_indoor"),
                ]).alias("indoor"),
                pl.coalesce([
                    pl.col("event_type"),
                    pl.col("_act_event_type"),
                ]).alias("event_type"),
            ]).drop([
                "_act_start_date", "_act_end_date",
                "_act_surface", "_act_indoor", "_act_event_type",
            ])

        return result

    def _activity_gap_fill(
        self, matches: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """Get Activity rows not in tournament matches and map to matches schema."""
        if activity.is_empty():
            return pl.DataFrame()
        existing_uids = (
            set(matches["match_uid"].unique().to_list())
            if not matches.is_empty()
            else set()
        )
        gap = activity.filter(~pl.col("match_uid").is_in(list(existing_uids)))
        if gap.is_empty():
            return pl.DataFrame()
        return map_activity_to_matches_schema(gap)

    def _load_rankings(self) -> pl.DataFrame | None:
        """Load consolidated rankings parquet."""
        path = (
            self.data_root
            / "stage"
            / "atptour"
            / "rankings"
            / "rankings_singles.parquet"
        )
        if not path.exists():
            return None
        return pl.read_parquet(path)

    def _load_bio(self) -> pl.DataFrame | None:
        """Load all player bio parquets into one DataFrame."""
        bio_dir = self.data_root / "stage" / "atptour" / "players"
        if not bio_dir.is_dir():
            return None
        files = sorted(bio_dir.glob("*.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    def run(self) -> Path | None:
        """Aggregate and write to parquet."""
        df = self.aggregate()
        if df.is_empty():
            logger.info("No matches to aggregate")
            return None
        out_path = self.build_path("aggregate", "", "matches.parquet")
        return self.save_parquet(df, out_path)
