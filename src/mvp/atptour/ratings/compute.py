"""Compute all ratings (Elo + Glicko-2) for a DataFrame of matches."""


import logging
from datetime import date

import polars as pl

from mvp.atptour.elo.compute import ELO_COLUMNS
from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    DEFAULT_RD,
    REVERSION_RATE,
    SERVE_RETURN_K_MULT,
    SURFACE_K_MULT,
)
from mvp.atptour.elo.ratings import (
    PlayerRating,
    apply_inactivity_rd,
    get_k_factor,
    initialize_player,
    update_ace_resistance,
    update_elo,
    update_first_serve_power,
    update_indoor_adj,
    update_rd,
    update_return_clutch,
    update_second_serve_reliability,
    update_serve_clutch,
    update_serve_elo,
    update_surface_adj,
    update_tb_clutch,
)

logger = logging.getLogger(__name__)

ALL_RATING_COLUMNS = ELO_COLUMNS


def _capture_elo_values(rating: PlayerRating) -> dict[str, float]:
    """Capture current rating values as a dict for caching/output.

    Keys match column suffixes so they can be prefixed with player_/opp_.
    """
    return {
        "elo": rating.elo,
        "elo_rd": rating.rd,
        "hard_adj": rating.hard_adj,
        "clay_adj": rating.clay_adj,
        "grass_adj": rating.grass_adj,
        "serve_elo": rating.serve_elo,
        "serve_elo_rd": rating.serve_rd,
        "return_elo": rating.return_elo,
        "return_elo_rd": rating.return_rd,
        "first_serve_power": rating.first_serve_power,
        "second_serve_reliability": rating.second_serve_reliability,
        "ace_resistance": rating.ace_resistance,
        "serve_clutch": rating.serve_clutch,
        "return_clutch": rating.return_clutch,
        "tb_clutch": rating.tb_clutch,
        "overall_clutch": rating.overall_clutch,
        "indoor_adj": rating.indoor_adj,
    }


def _append_ratings_to_output(
    output: dict[str, list],
    elo_player: dict[str, float],
    elo_opp: dict[str, float],
) -> None:
    """Append pre-match rating values to the output dict."""
    for key in elo_player:
        output[f"player_{key}"].append(elo_player[key])
        output[f"opp_{key}"].append(elo_opp[key])


def _count_tiebreaks(row: dict) -> tuple[int, int]:
    """Count tiebreaks won and played from set scores."""
    tb_won = 0
    tb_played = 0
    for i in range(1, 6):
        player_tb = row.get(f"player_set{i}_tiebreak")
        opp_tb = row.get(f"opp_set{i}_tiebreak")
        if player_tb is not None and opp_tb is not None:
            tb_played += 1
            if player_tb > opp_tb:
                tb_won += 1
    return tb_won, tb_played



def compute_all_ratings(df: pl.DataFrame) -> pl.DataFrame:
    """Add all rating columns to matches DataFrame.

    Iterates through matches chronologically, tracking player ratings
    and outputting pre-match values for each row.

    Args:
        df: DataFrame with matches, must have effective_match_date column.

    Returns:
        DataFrame with additional rating columns.
    """
    df = df.sort("effective_match_date")

    elo_ratings: dict[str, PlayerRating] = {}
    output: dict[str, list[float | None]] = {col: [] for col in ALL_RATING_COLUMNS}
    processed_matches: set[str] = set()
    # Cache pre-match ratings for each match_uid to handle both rows consistently
    match_ratings_cache: dict[str, dict[str, dict[str, float]]] = {}

    rows = df.to_dicts()
    for row in rows:
        match_uid = row["match_uid"]

        # Guard against None match_uid — would cause cache collisions
        if match_uid is None:
            logger.warning("Skipping row with None match_uid: %s", row.get("player_id"))
            for col in ALL_RATING_COLUMNS:
                output[col].append(None)
            continue

        player_id = row["player_id"]
        opp_id = row["opp_id"]
        surface = row.get("surface") or "Hard"
        round_name = row.get("round") or "R32"
        tournament_level = row.get("tournament_level") or "250"
        match_date = row["effective_match_date"]
        won = row["won"]

        # Initialize players if new
        if player_id not in elo_ratings:
            ranking = row.get("player_rank")
            elo_ratings[player_id] = initialize_player(ranking)
        if opp_id not in elo_ratings:
            opp_ranking = row.get("opp_rank")
            elo_ratings[opp_id] = initialize_player(opp_ranking)

        player_rating = elo_ratings[player_id]
        opp_rating = elo_ratings[opp_id]

        # Check if this match was already processed (second row of same match)
        if match_uid in match_ratings_cache:
            cached = match_ratings_cache.pop(match_uid)
            _append_ratings_to_output(output, cached[player_id], cached[opp_id])
            continue

        # First row for this match - apply inactivity and cache pre-match values
        if isinstance(match_date, date):
            player_rating.rd = apply_inactivity_rd(
                player_rating.rd, player_rating.last_match_date, match_date
            )
            player_rating.serve_rd = apply_inactivity_rd(
                player_rating.serve_rd, player_rating.last_match_date, match_date
            )
            player_rating.return_rd = apply_inactivity_rd(
                player_rating.return_rd, player_rating.last_match_date, match_date
            )
            opp_rating.rd = apply_inactivity_rd(
                opp_rating.rd, opp_rating.last_match_date, match_date
            )
            opp_rating.serve_rd = apply_inactivity_rd(
                opp_rating.serve_rd, opp_rating.last_match_date, match_date
            )
            opp_rating.return_rd = apply_inactivity_rd(
                opp_rating.return_rd, opp_rating.last_match_date, match_date
            )

        # Cache pre-match values for both players
        elo_player = _capture_elo_values(player_rating)
        elo_opp = _capture_elo_values(opp_rating)
        match_ratings_cache[match_uid] = {player_id: elo_player, opp_id: elo_opp}

        # Record PRE-MATCH values
        _append_ratings_to_output(output, elo_player, elo_opp)

        # Mark as processed and update ratings
        processed_matches.add(match_uid)

        # Per-player K-factors
        k_player = get_k_factor(player_rating, round_name, tournament_level)
        k_opp = get_k_factor(opp_rating, round_name, tournament_level)

        # Snapshot pre-match effective Elos
        player_effective = player_rating.effective_surface_elo(surface)
        opp_effective = opp_rating.effective_surface_elo(surface)

        # Update base Elo (both use pre-match snapshot)
        player_rating.elo = update_elo(
            player_rating.elo, player_effective, opp_effective, won, k_player
        )
        opp_rating.elo = update_elo(
            opp_rating.elo, opp_effective, player_effective, not won, k_opp
        )

        # Update surface adjustments using base Elos (not effective) to avoid
        # double-counting the surface signal already embedded in effective Elos
        if surface in ("Hard", "Clay", "Grass"):
            k_surface_player = k_player * SURFACE_K_MULT
            k_surface_opp = k_opp * SURFACE_K_MULT
            new_adj = update_surface_adj(
                player_rating.get_surface_adj(surface),
                player_rating.elo, opp_rating.elo, won, k_surface_player,
            )
            opp_new_adj = update_surface_adj(
                opp_rating.get_surface_adj(surface),
                opp_rating.elo, player_rating.elo, not won, k_surface_opp,
            )
            if surface == "Hard":
                player_rating.hard_adj = new_adj
                opp_rating.hard_adj = opp_new_adj
            elif surface == "Clay":
                player_rating.clay_adj = new_adj
                opp_rating.clay_adj = opp_new_adj
            elif surface == "Grass":
                player_rating.grass_adj = new_adj
                opp_rating.grass_adj = opp_new_adj

        # Update serve/return Elo — two sub-games per match
        # Averaged K keeps serve/return zero-sum
        k_serve = (k_player + k_opp) / 2 * SERVE_RETURN_K_MULT

        # Sub-game 1: player serves, opponent returns
        serve_won = row.get("pts_service_pts_won")
        serve_played = row.get("pts_service_pts_played")
        player_serve_pct = None
        if serve_won is not None and serve_played and serve_played > 0:
            player_serve_pct = serve_won / serve_played

        player_rating.serve_elo, opp_rating.return_elo = update_serve_elo(
            player_rating.serve_elo, opp_rating.return_elo,
            player_serve_pct, surface, k_serve,
        )

        # Sub-game 2: opponent serves, player returns
        opp_serve_won = row.get("opp_pts_service_pts_won")
        opp_serve_played = row.get("opp_pts_service_pts_played")
        opp_serve_pct = None
        if opp_serve_won is not None and opp_serve_played and opp_serve_played > 0:
            opp_serve_pct = opp_serve_won / opp_serve_played

        opp_rating.serve_elo, player_rating.return_elo = update_serve_elo(
            opp_rating.serve_elo, player_rating.return_elo,
            opp_serve_pct, surface, k_serve,
        )

        # Update style dimensions for BOTH players

        # --- Player style updates ---

        # First serve power: aces / first_serve_pts_won
        svc_aces = row.get("svc_aces")
        svc_first_serve_pts_won = row.get("svc_first_serve_pts_won")
        ace_rate = None
        if (svc_aces is not None
                and svc_first_serve_pts_won
                and svc_first_serve_pts_won > 0):
            ace_rate = svc_aces / svc_first_serve_pts_won
        player_rating.first_serve_power = update_first_serve_power(
            player_rating.first_serve_power, ace_rate, surface
        )

        # Second serve reliability: 1 - (DFs / second_serve_pts_played)
        svc_double_faults = row.get("svc_double_faults")
        svc_second_serve_pts_played = row.get("svc_second_serve_pts_played")
        reliability = None
        if (svc_double_faults is not None
                and svc_second_serve_pts_played
                and svc_second_serve_pts_played > 0):
            reliability = (
                1 - svc_double_faults / svc_second_serve_pts_played
            )
        player_rating.second_serve_reliability = update_second_serve_reliability(
            player_rating.second_serve_reliability, reliability, surface
        )

        # Ace resistance: 1 - (opp_svc_aces / ret_first_serve_pts_lost)
        opp_svc_aces = row.get("opp_svc_aces")
        ret_first_serve_pts_played = row.get("ret_first_serve_pts_played")
        ret_first_serve_pts_won = row.get("ret_first_serve_pts_won")
        ace_resistance_val = None
        if (opp_svc_aces is not None and
            ret_first_serve_pts_played is not None and
            ret_first_serve_pts_won is not None):
            ret_lost = ret_first_serve_pts_played - ret_first_serve_pts_won
            if ret_lost > 0:
                ace_resistance_val = 1 - (opp_svc_aces / ret_lost)
        player_rating.ace_resistance = update_ace_resistance(
            player_rating.ace_resistance, ace_resistance_val, surface
        )

        # Serve clutch: bp_saved / bp_faced
        svc_bp_saved = row.get("svc_bp_saved")
        svc_bp_faced = row.get("svc_bp_faced")
        save_rate = None
        if svc_bp_saved is not None and svc_bp_faced and svc_bp_faced > 0:
            save_rate = svc_bp_saved / svc_bp_faced
        player_rating.serve_clutch = update_serve_clutch(
            player_rating.serve_clutch, save_rate, surface
        )

        # Return clutch: bp_converted / bp_opportunities
        ret_bp_converted = row.get("ret_bp_converted")
        ret_bp_opportunities = row.get("ret_bp_opportunities")
        conversion_rate = None
        if (ret_bp_converted is not None
                and ret_bp_opportunities
                and ret_bp_opportunities > 0):
            conversion_rate = ret_bp_converted / ret_bp_opportunities
        player_rating.return_clutch = update_return_clutch(
            player_rating.return_clutch, conversion_rate, surface
        )

        # TB clutch: count won/played from set scores
        tb_won, tb_played = _count_tiebreaks(row)
        player_rating.tb_clutch = update_tb_clutch(
            player_rating.tb_clutch, tb_won, tb_played
        )

        # Indoor adjustment
        indoor = row.get("indoor", False)
        if indoor:
            player_rating.indoor_adj = update_indoor_adj(
                player_rating.indoor_adj, won
            )

        # --- Opponent style updates (mirror columns) ---

        opp_svc_first_serve_pts_won = row.get("opp_svc_first_serve_pts_won")
        opp_ace_rate = None
        if (opp_svc_aces is not None
                and opp_svc_first_serve_pts_won
                and opp_svc_first_serve_pts_won > 0):
            opp_ace_rate = opp_svc_aces / opp_svc_first_serve_pts_won
        opp_rating.first_serve_power = update_first_serve_power(
            opp_rating.first_serve_power, opp_ace_rate, surface
        )

        opp_svc_double_faults = row.get("opp_svc_double_faults")
        opp_svc_second_serve_pts_played = row.get("opp_svc_second_serve_pts_played")
        opp_reliability = None
        if (opp_svc_double_faults is not None
                and opp_svc_second_serve_pts_played
                and opp_svc_second_serve_pts_played > 0):
            opp_reliability = (
                1 - opp_svc_double_faults / opp_svc_second_serve_pts_played
            )
        opp_rating.second_serve_reliability = update_second_serve_reliability(
            opp_rating.second_serve_reliability, opp_reliability, surface
        )

        opp_ret_first_serve_pts_played = row.get("opp_ret_first_serve_pts_played")
        opp_ret_first_serve_pts_won = row.get("opp_ret_first_serve_pts_won")
        opp_ace_resistance_val = None
        if (svc_aces is not None and
            opp_ret_first_serve_pts_played is not None and
            opp_ret_first_serve_pts_won is not None):
            opp_ret_lost = opp_ret_first_serve_pts_played - opp_ret_first_serve_pts_won
            if opp_ret_lost > 0:
                opp_ace_resistance_val = 1 - (svc_aces / opp_ret_lost)
        opp_rating.ace_resistance = update_ace_resistance(
            opp_rating.ace_resistance, opp_ace_resistance_val, surface
        )

        opp_svc_bp_saved = row.get("opp_svc_bp_saved")
        opp_svc_bp_faced = row.get("opp_svc_bp_faced")
        opp_save_rate = None
        if opp_svc_bp_saved is not None and opp_svc_bp_faced and opp_svc_bp_faced > 0:
            opp_save_rate = opp_svc_bp_saved / opp_svc_bp_faced
        opp_rating.serve_clutch = update_serve_clutch(
            opp_rating.serve_clutch, opp_save_rate, surface
        )

        opp_ret_bp_converted = row.get("opp_ret_bp_converted")
        opp_ret_bp_opportunities = row.get("opp_ret_bp_opportunities")
        opp_conversion_rate = None
        if (opp_ret_bp_converted is not None
                and opp_ret_bp_opportunities
                and opp_ret_bp_opportunities > 0):
            opp_conversion_rate = opp_ret_bp_converted / opp_ret_bp_opportunities
        opp_rating.return_clutch = update_return_clutch(
            opp_rating.return_clutch, opp_conversion_rate, surface
        )

        opp_tb_won = tb_played - tb_won
        opp_rating.tb_clutch = update_tb_clutch(
            opp_rating.tb_clutch, opp_tb_won, tb_played
        )

        if indoor:
            opp_rating.indoor_adj = update_indoor_adj(
                opp_rating.indoor_adj, not won
            )

        # Mean reversion — counteract inflation from player turnover
        # Scaled by RD: uncertain players revert more, established players barely
        for r in (player_rating, opp_rating):
            reversion = REVERSION_RATE * (r.rd / DEFAULT_RD)
            r.elo += reversion * (DEFAULT_ELO - r.elo)
            r.hard_adj *= 1 - reversion
            r.clay_adj *= 1 - reversion
            r.grass_adj *= 1 - reversion

        # Update RD (decreases after match)
        player_rating.rd = update_rd(player_rating.rd)
        player_rating.serve_rd = update_rd(player_rating.serve_rd)
        player_rating.return_rd = update_rd(player_rating.return_rd)
        opp_rating.rd = update_rd(opp_rating.rd)
        opp_rating.serve_rd = update_rd(opp_rating.serve_rd)
        opp_rating.return_rd = update_rd(opp_rating.return_rd)

        # Update metadata
        player_rating.match_count += 1
        opp_rating.match_count += 1
        if isinstance(match_date, date):
            player_rating.last_match_date = match_date
            opp_rating.last_match_date = match_date

    # Add columns to DataFrame
    for col_name, values in output.items():
        df = df.with_columns(pl.Series(name=col_name, values=values))

    logger.info(
        "Computed ratings for %d players across %d unique matches (%d rows)",
        len(elo_ratings),
        len(processed_matches),
        len(df),
    )
    return df
