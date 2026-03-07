"""Compute Elo ratings for a DataFrame of matches."""


import logging
from datetime import date

import polars as pl

from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    REVERSION_RATE,
    SERVE_RETURN_K_MULT,
    STYLE_K_MULT,
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

STYLE_COLUMNS = [
    "player_first_serve_power",
    "player_second_serve_reliability",
    "player_ace_resistance",
    "player_serve_clutch",
    "player_return_clutch",
    "player_tb_clutch",
    "player_overall_clutch",
    "player_indoor_adj",
    "opp_first_serve_power",
    "opp_second_serve_reliability",
    "opp_ace_resistance",
    "opp_serve_clutch",
    "opp_return_clutch",
    "opp_tb_clutch",
    "opp_overall_clutch",
    "opp_indoor_adj",
]

ELO_COLUMNS = [
    "player_elo",
    "player_elo_rd",
    "player_hard_adj",
    "player_clay_adj",
    "player_grass_adj",
    "player_serve_elo",
    "player_serve_elo_rd",
    "player_return_elo",
    "player_return_elo_rd",
    "opp_elo",
    "opp_elo_rd",
    "opp_hard_adj",
    "opp_clay_adj",
    "opp_grass_adj",
    "opp_serve_elo",
    "opp_serve_elo_rd",
    "opp_return_elo",
    "opp_return_elo_rd",
] + STYLE_COLUMNS


def _capture_rating_values(rating: PlayerRating) -> dict[str, float]:
    """Capture current rating values as a dict for caching."""
    return {
        "elo": rating.elo,
        "rd": rating.rd,
        "hard_adj": rating.hard_adj,
        "clay_adj": rating.clay_adj,
        "grass_adj": rating.grass_adj,
        "serve_elo": rating.serve_elo,
        "serve_rd": rating.serve_rd,
        "return_elo": rating.return_elo,
        "return_rd": rating.return_rd,
        "first_serve_power": rating.first_serve_power,
        "second_serve_reliability": rating.second_serve_reliability,
        "ace_resistance": rating.ace_resistance,
        "serve_clutch": rating.serve_clutch,
        "return_clutch": rating.return_clutch,
        "tb_clutch": rating.tb_clutch,
        "overall_clutch": rating.overall_clutch,
        "indoor_adj": rating.indoor_adj,
    }


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


def compute_elo_ratings(df: pl.DataFrame) -> pl.DataFrame:
    """Add Elo rating columns to matches DataFrame.

    Iterates through matches chronologically, tracking player ratings
    and outputting pre-match values for each row.

    Args:
        df: DataFrame with matches, must have effective_match_date column.

    Returns:
        DataFrame with additional Elo columns.
    """
    df = df.sort("effective_match_date")

    ratings: dict[str, PlayerRating] = {}
    output: dict[str, list[float | None]] = {col: [] for col in ELO_COLUMNS}
    processed_matches: set[str] = set()
    # Cache pre-match ratings for each match_uid to handle both rows consistently
    match_ratings_cache: dict[str, dict[str, dict[str, float]]] = {}

    for row in df.iter_rows(named=True):
        match_uid = row["match_uid"]
        player_id = row["player_id"]
        opp_id = row["opp_id"]
        surface = row.get("surface") or "Hard"
        round_name = row.get("round") or "R32"
        tournament_level = row.get("tournament_level") or "250"
        match_date = row["effective_match_date"]
        won = row["won"]

        # Initialize players if new
        if player_id not in ratings:
            ranking = row.get("player_rank")
            ratings[player_id] = initialize_player(ranking)
        if opp_id not in ratings:
            opp_ranking = row.get("opp_rank")
            ratings[opp_id] = initialize_player(opp_ranking)

        player_rating = ratings[player_id]
        opp_rating = ratings[opp_id]

        # Check if this match was already processed (second row of same match)
        if match_uid in match_ratings_cache:
            # Use cached pre-match values for consistency
            cached = match_ratings_cache[match_uid]
            player_cached = cached[player_id]
            opp_cached = cached[opp_id]

            output["player_elo"].append(player_cached["elo"])
            output["player_elo_rd"].append(player_cached["rd"])
            output["player_hard_adj"].append(player_cached["hard_adj"])
            output["player_clay_adj"].append(player_cached["clay_adj"])
            output["player_grass_adj"].append(player_cached["grass_adj"])
            output["player_serve_elo"].append(player_cached["serve_elo"])
            output["player_serve_elo_rd"].append(player_cached["serve_rd"])
            output["player_return_elo"].append(player_cached["return_elo"])
            output["player_return_elo_rd"].append(player_cached["return_rd"])

            output["opp_elo"].append(opp_cached["elo"])
            output["opp_elo_rd"].append(opp_cached["rd"])
            output["opp_hard_adj"].append(opp_cached["hard_adj"])
            output["opp_clay_adj"].append(opp_cached["clay_adj"])
            output["opp_grass_adj"].append(opp_cached["grass_adj"])
            output["opp_serve_elo"].append(opp_cached["serve_elo"])
            output["opp_serve_elo_rd"].append(opp_cached["serve_rd"])
            output["opp_return_elo"].append(opp_cached["return_elo"])
            output["opp_return_elo_rd"].append(opp_cached["return_rd"])

            # Style dimensions (cached)
            output["player_first_serve_power"].append(player_cached["first_serve_power"])
            output["player_second_serve_reliability"].append(player_cached["second_serve_reliability"])
            output["player_ace_resistance"].append(player_cached["ace_resistance"])
            output["player_serve_clutch"].append(player_cached["serve_clutch"])
            output["player_return_clutch"].append(player_cached["return_clutch"])
            output["player_tb_clutch"].append(player_cached["tb_clutch"])
            output["player_overall_clutch"].append(player_cached["overall_clutch"])
            output["player_indoor_adj"].append(player_cached["indoor_adj"])

            output["opp_first_serve_power"].append(opp_cached["first_serve_power"])
            output["opp_second_serve_reliability"].append(opp_cached["second_serve_reliability"])
            output["opp_ace_resistance"].append(opp_cached["ace_resistance"])
            output["opp_serve_clutch"].append(opp_cached["serve_clutch"])
            output["opp_return_clutch"].append(opp_cached["return_clutch"])
            output["opp_tb_clutch"].append(opp_cached["tb_clutch"])
            output["opp_overall_clutch"].append(opp_cached["overall_clutch"])
            output["opp_indoor_adj"].append(opp_cached["indoor_adj"])
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
        match_ratings_cache[match_uid] = {
            player_id: _capture_rating_values(player_rating),
            opp_id: _capture_rating_values(opp_rating),
        }

        # Record PRE-MATCH values
        output["player_elo"].append(player_rating.elo)
        output["player_elo_rd"].append(player_rating.rd)
        output["player_hard_adj"].append(player_rating.hard_adj)
        output["player_clay_adj"].append(player_rating.clay_adj)
        output["player_grass_adj"].append(player_rating.grass_adj)
        output["player_serve_elo"].append(player_rating.serve_elo)
        output["player_serve_elo_rd"].append(player_rating.serve_rd)
        output["player_return_elo"].append(player_rating.return_elo)
        output["player_return_elo_rd"].append(player_rating.return_rd)

        output["opp_elo"].append(opp_rating.elo)
        output["opp_elo_rd"].append(opp_rating.rd)
        output["opp_hard_adj"].append(opp_rating.hard_adj)
        output["opp_clay_adj"].append(opp_rating.clay_adj)
        output["opp_grass_adj"].append(opp_rating.grass_adj)
        output["opp_serve_elo"].append(opp_rating.serve_elo)
        output["opp_serve_elo_rd"].append(opp_rating.serve_rd)
        output["opp_return_elo"].append(opp_rating.return_elo)
        output["opp_return_elo_rd"].append(opp_rating.return_rd)

        # Style dimensions (pre-match)
        output["player_first_serve_power"].append(player_rating.first_serve_power)
        output["player_second_serve_reliability"].append(player_rating.second_serve_reliability)
        output["player_ace_resistance"].append(player_rating.ace_resistance)
        output["player_serve_clutch"].append(player_rating.serve_clutch)
        output["player_return_clutch"].append(player_rating.return_clutch)
        output["player_tb_clutch"].append(player_rating.tb_clutch)
        output["player_overall_clutch"].append(player_rating.overall_clutch)
        output["player_indoor_adj"].append(player_rating.indoor_adj)

        output["opp_first_serve_power"].append(opp_rating.first_serve_power)
        output["opp_second_serve_reliability"].append(opp_rating.second_serve_reliability)
        output["opp_ace_resistance"].append(opp_rating.ace_resistance)
        output["opp_serve_clutch"].append(opp_rating.serve_clutch)
        output["opp_return_clutch"].append(opp_rating.return_clutch)
        output["opp_tb_clutch"].append(opp_rating.tb_clutch)
        output["opp_overall_clutch"].append(opp_rating.overall_clutch)
        output["opp_indoor_adj"].append(opp_rating.indoor_adj)

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

        # Update surface adjustments (using same pre-match snapshot)
        if surface in ("Hard", "Clay", "Grass"):
            k_surface_player = k_player * SURFACE_K_MULT
            k_surface_opp = k_opp * SURFACE_K_MULT
            new_adj = update_surface_adj(
                player_rating.get_surface_adj(surface),
                player_effective, opp_effective, won, k_surface_player,
            )
            opp_new_adj = update_surface_adj(
                opp_rating.get_surface_adj(surface),
                opp_effective, player_effective, not won, k_surface_opp,
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

        # Update style dimensions (per-player K, only updates one player by design)
        k_style = k_player * STYLE_K_MULT

        # First serve power: aces / first_serve_pts_won
        svc_aces = row.get("svc_aces")
        svc_first_serve_pts_won = row.get("svc_first_serve_pts_won")
        ace_rate = None
        if svc_aces is not None and svc_first_serve_pts_won and svc_first_serve_pts_won > 0:
            ace_rate = svc_aces / svc_first_serve_pts_won
        player_rating.first_serve_power = update_first_serve_power(
            player_rating.first_serve_power, ace_rate, surface, k_style
        )

        # Second serve reliability: 1 - (DFs / second_serve_pts_played)
        svc_double_faults = row.get("svc_double_faults")
        svc_second_serve_pts_played = row.get("svc_second_serve_pts_played")
        reliability = None
        if svc_double_faults is not None and svc_second_serve_pts_played and svc_second_serve_pts_played > 0:
            reliability = 1 - (svc_double_faults / svc_second_serve_pts_played)
        player_rating.second_serve_reliability = update_second_serve_reliability(
            player_rating.second_serve_reliability, reliability, surface, k_style
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
            player_rating.ace_resistance, ace_resistance_val, surface, k_style
        )

        # Serve clutch: bp_saved / bp_faced
        svc_bp_saved = row.get("svc_bp_saved")
        svc_bp_faced = row.get("svc_bp_faced")
        save_rate = None
        if svc_bp_saved is not None and svc_bp_faced and svc_bp_faced > 0:
            save_rate = svc_bp_saved / svc_bp_faced
        player_rating.serve_clutch = update_serve_clutch(
            player_rating.serve_clutch, save_rate, surface, k_style
        )

        # Return clutch: bp_converted / bp_opportunities
        ret_bp_converted = row.get("ret_bp_converted")
        ret_bp_opportunities = row.get("ret_bp_opportunities")
        conversion_rate = None
        if ret_bp_converted is not None and ret_bp_opportunities and ret_bp_opportunities > 0:
            conversion_rate = ret_bp_converted / ret_bp_opportunities
        player_rating.return_clutch = update_return_clutch(
            player_rating.return_clutch, conversion_rate, surface, k_style
        )

        # TB clutch: count won/played from set scores
        tb_won, tb_played = _count_tiebreaks(row)
        player_rating.tb_clutch = update_tb_clutch(
            player_rating.tb_clutch, tb_won, tb_played, k_style
        )

        # Overall clutch = average of serve, return, tb clutch
        player_rating.overall_clutch = (
            player_rating.serve_clutch +
            player_rating.return_clutch +
            player_rating.tb_clutch
        ) / 3

        # Indoor adjustment
        indoor = row.get("indoor", False)
        if indoor:
            player_rating.indoor_adj = update_indoor_adj(
                player_rating.indoor_adj, won, k_style
            )

        # Mean reversion — counteract inflation from player turnover
        player_rating.elo += REVERSION_RATE * (DEFAULT_ELO - player_rating.elo)
        opp_rating.elo += REVERSION_RATE * (DEFAULT_ELO - opp_rating.elo)

        for attr in ("hard_adj", "clay_adj", "grass_adj"):
            setattr(player_rating, attr, getattr(player_rating, attr) * (1 - REVERSION_RATE))
            setattr(opp_rating, attr, getattr(opp_rating, attr) * (1 - REVERSION_RATE))

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
        "Computed Elo for %d players across %d matches",
        len(ratings),
        len(processed_matches),
    )
    return df
