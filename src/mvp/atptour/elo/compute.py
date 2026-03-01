"""Compute Elo ratings for a DataFrame of matches."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from mvp.atptour.elo.constants import SERVE_RETURN_K_MULT, SURFACE_K_MULT
from mvp.atptour.elo.ratings import (
    PlayerRating,
    apply_inactivity_rd,
    get_k_factor,
    initialize_player,
    update_elo,
    update_rd,
    update_return_elo,
    update_serve_elo,
    update_surface_adj,
)

logger = logging.getLogger(__name__)

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
]


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
    }


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
        match_date = row["effective_match_date"]
        won = row["won"]

        # Initialize players if new
        if player_id not in ratings:
            ranking = row.get("player_rankings_rank")
            ratings[player_id] = initialize_player(ranking)
        if opp_id not in ratings:
            opp_ranking = row.get("opp_rankings_rank")
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

        # Mark as processed and update ratings
        processed_matches.add(match_uid)

        # Calculate K-factors
        k = get_k_factor(player_rating, round_name)
        k_surface = k * SURFACE_K_MULT
        k_serve = k * SERVE_RETURN_K_MULT

        # Update overall Elo
        player_rating.elo = update_elo(player_rating, opp_rating, won, k, surface)
        opp_rating.elo = update_elo(opp_rating, player_rating, not won, k, surface)

        # Update surface adjustments
        if surface in ("Hard", "Clay", "Grass"):
            new_adj = update_surface_adj(
                player_rating, opp_rating, won, surface, k_surface
            )
            opp_new_adj = update_surface_adj(
                opp_rating, player_rating, not won, surface, k_surface
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

        # Update serve/return Elo
        serve_won = row.get("pts_service_pts_won")
        serve_played = row.get("pts_service_pts_played")
        return_won = row.get("pts_return_pts_won")
        return_played = row.get("pts_return_pts_played")

        if serve_won is not None and serve_played and serve_played > 0:
            serve_pct = serve_won / serve_played
            player_rating.serve_elo = update_serve_elo(
                player_rating.serve_elo, serve_pct, surface, k_serve
            )
        if return_won is not None and return_played and return_played > 0:
            return_pct = return_won / return_played
            player_rating.return_elo = update_return_elo(
                player_rating.return_elo, return_pct, surface, k_serve
            )

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
