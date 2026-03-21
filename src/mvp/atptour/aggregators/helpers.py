"""Helper functions for the aggregation layer."""

import polars as pl


def explode_to_player_match(
    df: pl.DataFrame,
    player_cols: dict[str, str],
    opp_cols: dict[str, str],
    shared_cols: list[str],
) -> pl.DataFrame:
    """Convert match-level DataFrame (p1/p2) to player-match rows (player/opp).

    Produces two rows per match: one where p1 is "player" and p2 is "opp",
    and one where p2 is "player" and p1 is "opp".

    Args:
        df: Match-level DataFrame with p1/p2 columns.
        player_cols: Maps p1 source columns to player output names.
            E.g. ``{"p1_id": "player_id", "p1_seed": "player_seed"}``.
        opp_cols: Maps p2 source columns to opp output names.
            E.g. ``{"p2_id": "opp_id", "p2_seed": "opp_seed"}``.
        shared_cols: Columns carried through unchanged (e.g. ``["match_uid"]``).

    Returns:
        DataFrame with ``shared_cols`` + player output columns + opp output columns.
        Original p1/p2 columns are dropped.
    """
    player_out_names = list(player_cols.values())
    opp_out_names = list(opp_cols.values())
    output_cols = shared_cols + player_out_names + opp_out_names

    if df.is_empty():
        # Build an empty DataFrame with the correct output schema.
        schema = {c: df.schema[c] for c in shared_cols}
        for src, out in player_cols.items():
            schema[out] = df.schema[src]
        for src, out in opp_cols.items():
            schema[out] = df.schema[src]
        return pl.DataFrame(schema=schema)

    p1_src_keys = list(player_cols.keys())
    p2_src_keys = list(opp_cols.keys())

    # Row 1: p1 = player, p2 = opp
    row1_renames = dict(zip(p1_src_keys, player_out_names))
    row1_renames.update(dict(zip(p2_src_keys, opp_out_names)))
    row1 = df.select(shared_cols + p1_src_keys + p2_src_keys).rename(row1_renames)

    # Row 2: p2 = player, p1 = opp (swap)
    row2_renames = dict(zip(p2_src_keys, player_out_names))
    row2_renames.update(dict(zip(p1_src_keys, opp_out_names)))
    row2 = df.select(shared_cols + p2_src_keys + p1_src_keys).rename(row2_renames)

    return pl.concat([row1, row2], how="vertical_relaxed").select(output_cols)


# Columns dropped from Results before explosion
_RESULTS_DROP_COLS = [
    "p1_country", "p2_country",
    "p1_partner_name", "p1_partner_country",
    "p2_partner_name", "p2_partner_country",
    "source_file", "parsed_at",
]

# Set score field suffixes: set1_games, set1_tiebreak, ..., set5_tiebreak
_SET_SCORE_SUFFIXES = [
    f"set{n}_{kind}"
    for n in range(1, 6)
    for kind in ("games", "tiebreak")
]


def explode_results(df: pl.DataFrame) -> pl.DataFrame:
    """Explode Results from match-level (p1/p2) to player-match rows.

    Drops name/country/partner-name/traceability columns, derives ``won``
    from ``winner_id``, and produces two rows per match via
    :func:`explode_to_player_match`.
    """
    df = df.drop([c for c in _RESULTS_DROP_COLS if c in df.columns])
    df = df.filter(pl.col("match_uid").is_not_null())

    player_cols: dict[str, str] = {
        "p1_id": "player_id",
        "p1_name": "player_display_name",
        "p1_seed": "player_seed",
        "p1_entry": "player_entry",
        "p1_partner_id": "player_partner_id",
    }
    opp_cols: dict[str, str] = {
        "p2_id": "opp_id",
        "p2_name": "opp_display_name",
        "p2_seed": "opp_seed",
        "p2_entry": "opp_entry",
        "p2_partner_id": "opp_partner_id",
    }
    for suffix in _SET_SCORE_SUFFIXES:
        player_cols[f"p1_{suffix}"] = f"player_{suffix}"
        opp_cols[f"p2_{suffix}"] = f"opp_{suffix}"

    shared_cols = [
        "match_uid", "tournament_id", "year", "circuit",
        "draw_type", "round", "match_id", "winner_id",
        "result_type", "duration_seconds",
        "tournament_start_date", "tournament_end_date",
    ]

    result = explode_to_player_match(df, player_cols, opp_cols, shared_cols)

    # Derive won and drop winner_id
    if result.is_empty():
        result = result.with_columns(pl.lit(None, dtype=pl.Boolean).alias("won"))
    else:
        result = result.with_columns(
            (pl.col("player_id") == pl.col("winner_id")).alias("won")
        )
    return result.drop("winner_id")


# All stat field suffixes for MatchStats (without p1_/p2_ prefix)
_SVC_STATS = [
    "svc_aces", "svc_double_faults",
    "svc_first_serve_in", "svc_first_serve_att",
    "svc_first_serve_pts_won", "svc_first_serve_pts_played",
    "svc_second_serve_pts_won", "svc_second_serve_pts_played",
    "svc_bp_saved", "svc_bp_faced",
    "svc_games_played", "svc_serve_rating",
]
_RET_STATS = [
    "ret_first_serve_pts_won", "ret_first_serve_pts_played",
    "ret_second_serve_pts_won", "ret_second_serve_pts_played",
    "ret_bp_converted", "ret_bp_opportunities",
    "ret_games_played", "ret_return_rating",
]
_PTS_STATS = [
    "pts_service_pts_won", "pts_service_pts_played",
    "pts_return_pts_won", "pts_return_pts_played",
    "pts_total_pts_won", "pts_total_pts_played",
]
_ALL_STATS = _SVC_STATS + _RET_STATS + _PTS_STATS

# Identity fields that get player/opp prefix in MatchStats
_MS_IDENTITY_FIELDS = ["id", "seed", "entry", "partner_id"]

# Shared (non-player-indexed) columns in MatchStats
_MS_SHARED_COLS = [
    "match_uid", "tournament_id", "year", "circuit",
    "draw_type", "round", "round_id", "match_id",
    "surface", "tournament_start_date", "tournament_end_date",
    "tournament_city", "prize_money", "currency",
    "draw_size_singles", "draw_size_doubles",
    "winner_id", "duration_seconds", "reason",
    "number_of_sets", "sets_played", "is_qualifier",
    "scoring_system", "court_name",
    "umpire_first_name", "umpire_last_name",
]


def explode_match_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Explode MatchStats from match-level (p1/p2) to player-match rows.

    Each player row gets that player's own service/return/points stats
    (not mirrored opponent stats). Identity fields (id, seed, entry,
    partner_id) are mapped to player_*/opp_* as usual.

    Built manually (not via :func:`explode_to_player_match`) because
    stat fields map to the same output names for both perspectives.
    """
    df = df.drop([c for c in ("source_file", "parsed_at") if c in df.columns])
    df = df.filter(pl.col("match_uid").is_not_null())

    # Build the p1-as-player rename map
    p1_renames: dict[str, str] = {}
    p2_renames: dict[str, str] = {}
    for field in _MS_IDENTITY_FIELDS:
        p1_renames[f"p1_{field}"] = f"player_{field}"
        p1_renames[f"p2_{field}"] = f"opp_{field}"
        p2_renames[f"p2_{field}"] = f"player_{field}"
        p2_renames[f"p1_{field}"] = f"opp_{field}"
    for stat in _ALL_STATS:
        p1_renames[f"p1_{stat}"] = stat           # player's own stats
        p1_renames[f"p2_{stat}"] = f"opp_{stat}"  # opponent's stats
        p2_renames[f"p2_{stat}"] = stat           # player's own stats
        p2_renames[f"p1_{stat}"] = f"opp_{stat}"  # opponent's stats

    # Determine columns to select for each perspective
    p1_select_cols = _MS_SHARED_COLS + list(p1_renames.keys())
    p2_select_cols = _MS_SHARED_COLS + list(p2_renames.keys())

    if df.is_empty():
        # Build empty schema
        schema: dict[str, pl.DataType] = {}
        for c in _MS_SHARED_COLS:
            if c in df.schema:
                schema[c] = df.schema[c]
        for src, out in p1_renames.items():
            if src in df.schema:
                schema[out] = df.schema[src]
        schema["won"] = pl.Boolean
        # Remove winner_id from output schema
        schema.pop("winner_id", None)
        return pl.DataFrame(schema=schema)

    # Row 1: p1 is player
    row1 = df.select(p1_select_cols).rename(p1_renames)
    # Row 2: p2 is player (only carry p2 stats, not p1 stats)
    row2 = df.select(p2_select_cols).rename(p2_renames)

    # Ensure consistent column order
    output_col_order = row1.columns
    row2 = row2.select(output_col_order)

    result = pl.concat([row1, row2], how="vertical_relaxed")

    # Derive won from winner_id (nullable)
    result = result.with_columns(
        pl.when(pl.col("winner_id").is_null())
        .then(None)
        .otherwise(pl.col("player_id") == pl.col("winner_id"))
        .alias("won")
    )
    return result.drop("winner_id")


# Columns dropped from Schedule before explosion
_SCHEDULE_DROP_COLS = [
    "source_file", "parsed_at", "snapshot_timestamp",
    "p1_country", "p2_country",
]


def explode_schedule(df: pl.DataFrame) -> pl.DataFrame:
    """Explode Schedule from match-level (p1/p2) to player-match rows.

    Drops name/country/traceability/snapshot columns and produces two
    rows per match. No ``won`` column — schedule doesn't know who won.
    """
    df = df.drop([c for c in _SCHEDULE_DROP_COLS if c in df.columns])
    df = df.filter(pl.col("match_uid").is_not_null())

    player_cols: dict[str, str] = {
        "p1_id": "player_id",
        "p1_name": "player_display_name",
        "p1_seed": "player_seed",
        "p1_entry": "player_entry",
    }
    opp_cols: dict[str, str] = {
        "p2_id": "opp_id",
        "p2_name": "opp_display_name",
        "p2_seed": "opp_seed",
        "p2_entry": "opp_entry",
    }
    shared_cols = [
        "match_uid", "tournament_id", "year", "circuit",
        "draw_type", "round", "match_date", "scheduled_datetime",
        "time_suffix", "display_time", "court_name",
        "court_match_num", "is_time_estimated", "status", "score",
    ]

    return explode_to_player_match(df, player_cols, opp_cols, shared_cols)
