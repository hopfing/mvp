"""Compute all ratings (Elo + Glicko-2) for a DataFrame of matches."""


import logging
from datetime import date

import polars as pl

from mvp.atptour.elo.compute import ELO_COLUMNS
from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    DEFAULT_RD,
    INDOOR_K_MULT,
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
from mvp.atptour.glicko.constants import (
    GLICKO_REVERSION_RATE,
    INITIAL_MU,
    INITIAL_RD,
    TAU,
)
from mvp.atptour.glicko.ratings import (
    GlickoRating,
    apply_glicko_inactivity,
    decay_glicko_rd,
    glicko2_update,
)

logger = logging.getLogger(__name__)

GLICKO_COLUMNS = [
    "player_glicko_mu", "player_glicko_rd", "player_glicko_sigma",
    "player_glicko_hard_rd", "player_glicko_clay_rd", "player_glicko_grass_rd",
    "opp_glicko_mu", "opp_glicko_rd", "opp_glicko_sigma",
    "opp_glicko_hard_rd", "opp_glicko_clay_rd", "opp_glicko_grass_rd",
]

ALL_RATING_COLUMNS = ELO_COLUMNS + GLICKO_COLUMNS


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


def _capture_glicko_values(rating: GlickoRating) -> dict[str, float]:
    """Capture current Glicko-2 rating values for caching/output."""
    return {
        "glicko_mu": rating.mu,
        "glicko_rd": rating.rd,
        "glicko_sigma": rating.sigma,
        "glicko_hard_rd": rating.hard_rd,
        "glicko_clay_rd": rating.clay_rd,
        "glicko_grass_rd": rating.grass_rd,
    }


def _append_ratings_to_output(
    output: dict[str, list],
    elo_player: dict[str, float],
    elo_opp: dict[str, float],
    glicko_player: dict[str, float],
    glicko_opp: dict[str, float],
) -> None:
    """Append pre-match rating values to the output dict."""
    for key in elo_player:
        output[f"player_{key}"].append(elo_player[key])
        output[f"opp_{key}"].append(elo_opp[key])
    for key in glicko_player:
        output[f"player_{key}"].append(glicko_player[key])
        output[f"opp_{key}"].append(glicko_opp[key])


def _count_tiebreaks(player_tbs: list, opp_tbs: list) -> tuple[int, int]:
    """Count tiebreaks won and played from set scores."""
    tb_won = 0
    tb_played = 0
    for player_tb, opp_tb in zip(player_tbs, opp_tbs):
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
    # Deterministic total order. On a same-day collision tournament_start_date
    # sorts the finishing event ahead of the one just starting; round_order then
    # keeps same-day rounds within a tournament in sequence — the earlier round
    # must update the chain first. match_uid/player_id make the order total so the
    # sequential rating chain can never depend on input row order.
    # tournament_start_date/round_order are carried by _RATINGS_INPUT_COLS upstream.
    df = df.sort(["effective_match_date", "tournament_start_date", "round_order", "match_uid", "player_id"])

    # Extract columns as Python lists to avoid 3GB .to_dicts() overhead.
    # Null values become None, matching the old row.get() behavior.
    n = len(df)
    df_cols = set(df.columns)

    def _col(name, default=None):
        if name not in df_cols:
            return [default] * n
        if default is not None:
            return df[name].fill_null(default).to_list()
        return df[name].to_list()

    col_match_uid = df["match_uid"].to_list()
    col_player_id = df["player_id"].to_list()
    col_opp_id = df["opp_id"].to_list()
    col_surface = _col("surface", "Hard")
    col_round = _col("round", "R32")
    col_tournament_level = _col("tournament_level", "250")
    col_match_date = df["effective_match_date"].to_list()
    col_won = df["won"].to_list()
    col_player_rank = _col("player_rank")
    col_opp_rank = _col("opp_rank")

    # Serve/return stat columns
    col_pts_service_pts_won = _col("pts_service_pts_won")
    col_pts_service_pts_played = _col("pts_service_pts_played")
    col_opp_pts_service_pts_won = _col("opp_pts_service_pts_won")
    col_opp_pts_service_pts_played = _col("opp_pts_service_pts_played")
    col_svc_aces = _col("svc_aces")
    col_svc_first_serve_pts_won = _col("svc_first_serve_pts_won")
    col_svc_double_faults = _col("svc_double_faults")
    col_svc_second_serve_pts_played = _col("svc_second_serve_pts_played")
    col_opp_svc_aces = _col("opp_svc_aces")
    col_ret_first_serve_pts_played = _col("ret_first_serve_pts_played")
    col_ret_first_serve_pts_won = _col("ret_first_serve_pts_won")
    col_svc_bp_saved = _col("svc_bp_saved")
    col_svc_bp_faced = _col("svc_bp_faced")
    col_ret_bp_converted = _col("ret_bp_converted")
    col_ret_bp_opportunities = _col("ret_bp_opportunities")
    col_indoor = _col("indoor", False)

    # Opponent mirror stat columns
    col_opp_svc_first_serve_pts_won = _col("opp_svc_first_serve_pts_won")
    col_opp_svc_double_faults = _col("opp_svc_double_faults")
    col_opp_svc_second_serve_pts_played = _col("opp_svc_second_serve_pts_played")
    col_opp_ret_first_serve_pts_played = _col("opp_ret_first_serve_pts_played")
    col_opp_ret_first_serve_pts_won = _col("opp_ret_first_serve_pts_won")
    col_opp_svc_bp_saved = _col("opp_svc_bp_saved")
    col_opp_svc_bp_faced = _col("opp_svc_bp_faced")
    col_opp_ret_bp_converted = _col("opp_ret_bp_converted")
    col_opp_ret_bp_opportunities = _col("opp_ret_bp_opportunities")

    # Tiebreak columns
    col_player_set_tb = [_col(f"player_set{s}_tiebreak") for s in range(1, 6)]
    col_opp_set_tb = [_col(f"opp_set{s}_tiebreak") for s in range(1, 6)]

    elo_ratings: dict[str, PlayerRating] = {}
    glicko_ratings: dict[str, GlickoRating] = {}
    output: dict[str, list[float | None]] = {col: [] for col in ALL_RATING_COLUMNS}
    processed_matches: set[str] = set()
    # Cache pre-match ratings for each match_uid to handle both rows consistently
    match_ratings_cache: dict[str, dict[str, dict[str, float]]] = {}

    for i in range(n):
        match_uid = col_match_uid[i]

        # Guard against None match_uid — would cause cache collisions
        if match_uid is None:
            logger.warning("Skipping row with None match_uid: %s", col_player_id[i])
            for col in ALL_RATING_COLUMNS:
                output[col].append(None)
            continue

        player_id = col_player_id[i]
        opp_id = col_opp_id[i]
        surface = col_surface[i]
        indoor = col_indoor[i]
        round_name = col_round[i]
        tournament_level = col_tournament_level[i]
        match_date = col_match_date[i]
        won = col_won[i]

        # Initialize players if new
        if player_id not in elo_ratings:
            ranking = col_player_rank[i]
            elo_ratings[player_id] = initialize_player(ranking)
            # Seed Glicko mu from the rank-based Elo seed (same Elo-point scale),
            # not flat INITIAL_MU — flat seeding overrates weak entrants and is a
            # primary driver of mu inflation.
            glicko_ratings[player_id] = GlickoRating(mu=elo_ratings[player_id].elo)
        if opp_id not in elo_ratings:
            opp_ranking = col_opp_rank[i]
            elo_ratings[opp_id] = initialize_player(opp_ranking)
            glicko_ratings[opp_id] = GlickoRating(mu=elo_ratings[opp_id].elo)

        player_rating = elo_ratings[player_id]
        opp_rating = elo_ratings[opp_id]

        # Check if this match was already processed (second row of same match)
        if match_uid in match_ratings_cache:
            cached = match_ratings_cache.pop(match_uid)
            p_cached = cached[player_id]
            o_cached = cached[opp_id]
            _append_ratings_to_output(
                output,
                p_cached["elo"], o_cached["elo"],
                p_cached["glicko"], o_cached["glicko"],
            )
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

        # Glicko-2 inactivity
        glicko_p = glicko_ratings[player_id]
        glicko_o = glicko_ratings[opp_id]

        if isinstance(match_date, date):
            glicko_p.rd = apply_glicko_inactivity(
                glicko_p.rd, glicko_p.sigma, glicko_p.last_match_date, match_date
            )
            glicko_o.rd = apply_glicko_inactivity(
                glicko_o.rd, glicko_o.sigma, glicko_o.last_match_date, match_date
            )
            # Surface RD grows with inactivity using base sigma
            for surf in ("hard", "clay", "grass"):
                for r in (glicko_p, glicko_o):
                    setattr(r, f"{surf}_rd", apply_glicko_inactivity(
                        getattr(r, f"{surf}_rd"),
                        r.sigma,
                        getattr(r, f"last_{surf}_date"),
                        match_date,
                    ))

        # Cache pre-match values for both players
        elo_player = _capture_elo_values(player_rating)
        elo_opp = _capture_elo_values(opp_rating)
        glicko_p_vals = _capture_glicko_values(glicko_p)
        glicko_o_vals = _capture_glicko_values(glicko_o)

        match_ratings_cache[match_uid] = {
            player_id: {"elo": elo_player, "glicko": glicko_p_vals},
            opp_id: {"elo": elo_opp, "glicko": glicko_o_vals},
        }

        # Record PRE-MATCH values
        _append_ratings_to_output(
            output, elo_player, elo_opp, glicko_p_vals, glicko_o_vals,
        )

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

        # Update surface adjustments using pre-match effective Elos (same snapshot)
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

        # Update indoor adjustment — additive and opponent-adjusted, the same
        # mechanic as the surface adjustments, only on indoor matches. Narrow
        # nesting: indoor_adj uses its own indoor-inclusive effective (base +
        # surface + pre-match indoor) for its expected score, but that effective
        # does NOT feed the base-Elo / surface-adj updates above (those stay on
        # the surface-only snapshot). Both sides snapshot pre-match indoor_adj
        # before either is reassigned.
        if indoor:
            indoor_eff_player = player_effective + player_rating.indoor_adj
            indoor_eff_opp = opp_effective + opp_rating.indoor_adj
            new_indoor_adj = update_indoor_adj(
                player_rating.indoor_adj,
                indoor_eff_player, indoor_eff_opp, won, k_player * INDOOR_K_MULT,
            )
            opp_new_indoor_adj = update_indoor_adj(
                opp_rating.indoor_adj,
                indoor_eff_opp, indoor_eff_player, not won, k_opp * INDOOR_K_MULT,
            )
            player_rating.indoor_adj = new_indoor_adj
            opp_rating.indoor_adj = opp_new_indoor_adj

        # Update serve/return Elo — two sub-games per match
        # Averaged K keeps serve/return zero-sum
        k_serve = (k_player + k_opp) / 2 * SERVE_RETURN_K_MULT

        # Sub-game 1: player serves, opponent returns
        serve_won = col_pts_service_pts_won[i]
        serve_played = col_pts_service_pts_played[i]
        player_serve_pct = None
        if serve_won is not None and serve_played and serve_played > 0:
            player_serve_pct = serve_won / serve_played

        player_rating.serve_elo, opp_rating.return_elo = update_serve_elo(
            player_rating.serve_elo, opp_rating.return_elo,
            player_serve_pct, surface, k_serve,
        )

        # Sub-game 2: opponent serves, player returns
        opp_serve_won = col_opp_pts_service_pts_won[i]
        opp_serve_played = col_opp_pts_service_pts_played[i]
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
        svc_aces = col_svc_aces[i]
        svc_first_serve_pts_won = col_svc_first_serve_pts_won[i]
        ace_rate = None
        if (svc_aces is not None
                and svc_first_serve_pts_won
                and svc_first_serve_pts_won > 0):
            ace_rate = svc_aces / svc_first_serve_pts_won
        player_rating.first_serve_power = update_first_serve_power(
            player_rating.first_serve_power, ace_rate, surface
        )

        # Second serve reliability: 1 - (DFs / second_serve_pts_played)
        svc_double_faults = col_svc_double_faults[i]
        svc_second_serve_pts_played = col_svc_second_serve_pts_played[i]
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
        opp_svc_aces = col_opp_svc_aces[i]
        ret_first_serve_pts_played = col_ret_first_serve_pts_played[i]
        ret_first_serve_pts_won = col_ret_first_serve_pts_won[i]
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
        svc_bp_saved = col_svc_bp_saved[i]
        svc_bp_faced = col_svc_bp_faced[i]
        save_rate = None
        if svc_bp_saved is not None and svc_bp_faced and svc_bp_faced > 0:
            save_rate = svc_bp_saved / svc_bp_faced
        player_rating.serve_clutch = update_serve_clutch(
            player_rating.serve_clutch, save_rate, surface
        )

        # Return clutch: bp_converted / bp_opportunities
        ret_bp_converted = col_ret_bp_converted[i]
        ret_bp_opportunities = col_ret_bp_opportunities[i]
        conversion_rate = None
        if (ret_bp_converted is not None
                and ret_bp_opportunities
                and ret_bp_opportunities > 0):
            conversion_rate = ret_bp_converted / ret_bp_opportunities
        player_rating.return_clutch = update_return_clutch(
            player_rating.return_clutch, conversion_rate, surface
        )

        # TB clutch: count won/played from set scores
        tb_won, tb_played = _count_tiebreaks(
            [col_player_set_tb[s][i] for s in range(5)],
            [col_opp_set_tb[s][i] for s in range(5)],
        )
        player_rating.tb_clutch = update_tb_clutch(
            player_rating.tb_clutch, tb_won, tb_played
        )

        # Overall clutch = average of serve, return, tb clutch
        player_rating.overall_clutch = (
            player_rating.serve_clutch +
            player_rating.return_clutch +
            player_rating.tb_clutch
        ) / 3

        # --- Opponent style updates (mirror columns) ---

        opp_svc_first_serve_pts_won = col_opp_svc_first_serve_pts_won[i]
        opp_ace_rate = None
        if (opp_svc_aces is not None
                and opp_svc_first_serve_pts_won
                and opp_svc_first_serve_pts_won > 0):
            opp_ace_rate = opp_svc_aces / opp_svc_first_serve_pts_won
        opp_rating.first_serve_power = update_first_serve_power(
            opp_rating.first_serve_power, opp_ace_rate, surface
        )

        opp_svc_double_faults = col_opp_svc_double_faults[i]
        opp_svc_second_serve_pts_played = col_opp_svc_second_serve_pts_played[i]
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

        opp_ret_first_serve_pts_played = col_opp_ret_first_serve_pts_played[i]
        opp_ret_first_serve_pts_won = col_opp_ret_first_serve_pts_won[i]
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

        opp_svc_bp_saved = col_opp_svc_bp_saved[i]
        opp_svc_bp_faced = col_opp_svc_bp_faced[i]
        opp_save_rate = None
        if opp_svc_bp_saved is not None and opp_svc_bp_faced and opp_svc_bp_faced > 0:
            opp_save_rate = opp_svc_bp_saved / opp_svc_bp_faced
        opp_rating.serve_clutch = update_serve_clutch(
            opp_rating.serve_clutch, opp_save_rate, surface
        )

        opp_ret_bp_converted = col_opp_ret_bp_converted[i]
        opp_ret_bp_opportunities = col_opp_ret_bp_opportunities[i]
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

        opp_rating.overall_clutch = (
            opp_rating.serve_clutch +
            opp_rating.return_clutch +
            opp_rating.tb_clutch
        ) / 3

        # Mean reversion — counteract inflation from player turnover
        # Scaled by RD: uncertain players revert more, established players barely
        for r in (player_rating, opp_rating):
            reversion = REVERSION_RATE * (r.rd / DEFAULT_RD)
            r.elo += reversion * (DEFAULT_ELO - r.elo)
            r.hard_adj *= 1 - reversion
            r.clay_adj *= 1 - reversion
            r.grass_adj *= 1 - reversion
            r.indoor_adj *= 1 - reversion

        # Update RD (decreases after match)
        player_rating.rd = update_rd(player_rating.rd)
        player_rating.serve_rd = update_rd(player_rating.serve_rd)
        player_rating.return_rd = update_rd(player_rating.return_rd)
        opp_rating.rd = update_rd(opp_rating.rd)
        opp_rating.serve_rd = update_rd(opp_rating.serve_rd)
        opp_rating.return_rd = update_rd(opp_rating.return_rd)

        # === GLICKO-2 UPDATES ===
        # Snapshot pre-update values (both use pre-match state)
        pre_p_mu, pre_p_rd = glicko_p.mu, glicko_p.rd
        pre_p_sigma = glicko_p.sigma
        pre_o_mu, pre_o_rd = glicko_o.mu, glicko_o.rd
        pre_o_sigma = glicko_o.sigma

        # Base rating update
        glicko_p.mu, glicko_p.rd, glicko_p.sigma = glicko2_update(
            pre_p_mu, pre_p_rd, pre_p_sigma,
            pre_o_mu, pre_o_rd, won, TAU,
        )
        glicko_o.mu, glicko_o.rd, glicko_o.sigma = glicko2_update(
            pre_o_mu, pre_o_rd, pre_o_sigma,
            pre_p_mu, pre_p_rd, not won, TAU,
        )

        # Mean reversion on mu — counteract the non-conservation of the
        # phi^2-weighted asymmetric update under heterogeneous RD (see
        # glicko/ratings.py:165), the same turnover inflation the Elo block
        # above corrects. RD-scaled by the pre-match RD (uncertain players
        # revert most, converged players barely); own constant. Applied to the
        # post-update mu, before the next match's pre-match snapshot — PIT-safe.
        for g_r, pre_rd in ((glicko_p, pre_p_rd), (glicko_o, pre_o_rd)):
            g_reversion = GLICKO_REVERSION_RATE * (pre_rd / INITIAL_RD)
            g_r.mu += g_reversion * (INITIAL_MU - g_r.mu)

        # Surface RD decay — playing on a surface reduces uncertainty
        if surface in ("Hard", "Clay", "Grass"):
            surf_lower = surface.lower()
            rd_attr = f"{surf_lower}_rd"
            date_attr = f"last_{surf_lower}_date"

            setattr(glicko_p, rd_attr, decay_glicko_rd(getattr(glicko_p, rd_attr)))
            setattr(glicko_o, rd_attr, decay_glicko_rd(getattr(glicko_o, rd_attr)))

            if isinstance(match_date, date):
                setattr(glicko_p, date_attr, match_date)
                setattr(glicko_o, date_attr, match_date)

        # Glicko metadata
        glicko_p.match_count += 1
        glicko_o.match_count += 1
        if isinstance(match_date, date):
            glicko_p.last_match_date = match_date
            glicko_o.last_match_date = match_date

        # Update Elo metadata
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
