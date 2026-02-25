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
