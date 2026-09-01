"""Compute all ratings (Elo + Glicko-2) for a DataFrame of matches."""


import logging
from datetime import date

import polars as pl

from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    DEFAULT_RD,
    DEFAULT_SERVE_ELO_CONFIG,
    INDOOR_K_MULT,
    REVERSION_RATE,
    SERVE_SURFACE_K_MULT,
    SURFACE_K_MULT,
    ServeEloConfig,
)
from mvp.atptour.elo.mov import MovTracker, margin_is_valid
from mvp.atptour.elo.ratings import (
    PlayerRating,
    apply_inactivity_rd,
    get_k_factor,
    k_factor_from,
    serve_surprise,
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

GLICKO_COLUMNS = [
    "player_glicko_mu", "player_glicko_rd", "player_glicko_sigma",
    "player_glicko_hard_rd", "player_glicko_clay_rd", "player_glicko_grass_rd",
    "opp_glicko_mu", "opp_glicko_rd", "opp_glicko_sigma",
    "opp_glicko_hard_rd", "opp_glicko_clay_rd", "opp_glicko_grass_rd",
]

# Emitted only under `stamp`, never into the live matches.parquet. They exist
# so a sweep cell can segment on the SAME count that gated its K factor rather
# than on a reconstruction, which is an analysis need — not a reason to carry
# four more columns across 1.35M rows on every 15-minute pipeline run. Whether
# serve experience is worth having as a production feature is a separate
# question, and a feature-selection one.
SERVE_COUNT_COLUMNS = [
    "player_serve_match_count", "player_return_match_count",
    "opp_serve_match_count", "opp_return_match_count",
]

# Surface/venue serve adjustments and their rds. These SHIP LIVE, unlike
# pass one's counters. The counters were diagnostic — nothing consumed
# them — whereas these exist to be selectable features, and gating them
# would mean feature selection could never see the dimension they were
# built for. The rds ship for the same reason: this codebase already
# treats rating deviation as a feature category (elo_rd, serve_elo_rd,
# elo_rd_sum, svc_elo_matchup_rd), so holding these back would be an
# exception without a principle. Only the date clocks stay internal —
# nothing analogous to a raw date is a feature, and their effect is
# already fully expressed in the rd they drive.
SERVE_ADJ_COLUMNS = [
    "player_svc_hard_adj", "player_svc_hard_rd", "player_ret_hard_adj", "player_ret_hard_rd", "player_svc_clay_adj", "player_svc_clay_rd", "player_ret_clay_adj", "player_ret_clay_rd", "player_svc_grass_adj", "player_svc_grass_rd", "player_ret_grass_adj", "player_ret_grass_rd", "player_svc_indoor_adj", "player_svc_indoor_rd", "player_ret_indoor_adj", "player_ret_indoor_rd", "opp_svc_hard_adj", "opp_svc_hard_rd", "opp_ret_hard_adj", "opp_ret_hard_rd", "opp_svc_clay_adj", "opp_svc_clay_rd", "opp_ret_clay_adj", "opp_ret_clay_rd", "opp_svc_grass_adj", "opp_svc_grass_rd", "opp_ret_grass_adj", "opp_ret_grass_rd", "opp_svc_indoor_adj", "opp_svc_indoor_rd", "opp_ret_indoor_adj", "opp_ret_indoor_rd",
]

ALL_RATING_COLUMNS = ELO_COLUMNS + SERVE_ADJ_COLUMNS + GLICKO_COLUMNS


SERVE_AXES = ("hard", "clay", "grass", "indoor")


def _bump(rating: PlayerRating, field: str, delta: float) -> None:
    """Add to a named adjustment field. Keeps the axis loops readable."""
    setattr(rating, field, getattr(rating, field) + delta)


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
        # Captured so they ride the per-match cache; whether they are EMITTED
        # is decided by which columns `output` was built with.
        "serve_match_count": rating.serve_match_count,
        "return_match_count": rating.return_match_count,
        # Surface/venue adjustments and their own rds. Captured always so they
        # ride the per-match cache; emitted only under stamp. The date clocks
        # are deliberately absent — their effect is fully expressed in the rd.
        "svc_hard_adj": rating.svc_hard_adj,
        "svc_hard_rd": rating.svc_hard_rd,
        "ret_hard_adj": rating.ret_hard_adj,
        "ret_hard_rd": rating.ret_hard_rd,
        "svc_clay_adj": rating.svc_clay_adj,
        "svc_clay_rd": rating.svc_clay_rd,
        "ret_clay_adj": rating.ret_clay_adj,
        "ret_clay_rd": rating.ret_clay_rd,
        "svc_grass_adj": rating.svc_grass_adj,
        "svc_grass_rd": rating.svc_grass_rd,
        "ret_grass_adj": rating.ret_grass_adj,
        "ret_grass_rd": rating.ret_grass_rd,
        "svc_indoor_adj": rating.svc_indoor_adj,
        "svc_indoor_rd": rating.svc_indoor_rd,
        "ret_indoor_adj": rating.ret_indoor_adj,
        "ret_indoor_rd": rating.ret_indoor_rd,
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
    """Append pre-match rating values to the output dict.

    Keys absent from `output` are skipped rather than raising: the serve/return
    counters are captured on every run but only emitted for stamped ones, so
    the caller decides the column set and this stays agnostic to it.
    """
    for key in elo_player:
        if f"player_{key}" not in output:
            continue
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



def compute_all_ratings(
    df: pl.DataFrame,
    serve_config: ServeEloConfig | None = None,
    stamp: bool = False,
    mov_tracker: "MovTracker | None" = None,
) -> pl.DataFrame:
    """Add all rating columns to matches DataFrame.

    Iterates through matches chronologically, tracking player ratings
    and outputting pre-match values for each row.

    Args:
        df: DataFrame with matches, must have effective_match_date column.
        serve_config: serve/return knobs to run under. None keeps the module
            defaults, so the live pipeline is unaffected. Passed explicitly
            rather than patched into the constants module because these values
            are imported by name at load time — patching would leave the old
            bindings in place and silently produce the previous configuration.
        stamp: emit the resolved serve config as constant columns, so a sweep
            cell's output carries proof of what it actually ran under and
            cannot be mislabelled. Off by default to keep matches.parquet as
            it is.
        mov_tracker: optional margin-of-victory variant state (elo/mov.py).
            None (the default) leaves every existing code path and output
            column untouched — the pipeline-safety invariant, tested by
            asserting identical existing-column output with and without a
            tracker. When passed, the df must carry per-set games columns
            (plus `reason`/`result_type` for the incomplete guard) or this
            raises: silently falling back to binary on EVERY row would make
            the variants degenerate copies of standard elo.

    Returns:
        DataFrame with additional rating columns.
    """
    svc_cfg = serve_config or DEFAULT_SERVE_ELO_CONFIG
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

    # MOV variant inputs — materialized only when a tracker is running.
    if mov_tracker is not None:
        games_cols = [f"player_set{s}_games" for s in range(1, 6)]
        if not any(c in df_cols for c in games_cols):
            raise ValueError(
                "mov_tracker passed but the frame carries no per-set games "
                "columns — every update would silently fall back to binary "
                "and the variants would be degenerate copies of standard elo"
            )
        missing_guard = [
            c for c in ("reason", "result_type") if c not in df_cols
        ]
        if missing_guard:
            raise ValueError(
                f"mov_tracker passed but {missing_guard} absent — the "
                "incomplete guard would silently degrade to zero-games-only, "
                "and a retirement's nonzero partial margin would feed the "
                "update instead of falling back to binary"
            )
        col_player_set_games = [_col(f"player_set{s}_games") for s in range(1, 6)]
        col_opp_set_games = [_col(f"opp_set{s}_games") for s in range(1, 6)]
        col_reason = _col("reason")
        col_result_type = _col("result_type")

    elo_ratings: dict[str, PlayerRating] = {}
    glicko_ratings: dict[str, GlickoRating] = {}
    cols = ALL_RATING_COLUMNS + (SERVE_COUNT_COLUMNS if stamp else [])
    if mov_tracker is not None:
        cols = cols + mov_tracker.output_columns()
    output: dict[str, list[float | None]] = {col: [] for col in cols}
    processed_matches: set[str] = set()
    # Cache pre-match ratings for each match_uid to handle both rows consistently
    match_ratings_cache: dict[str, dict[str, dict[str, float]]] = {}

    for i in range(n):
        match_uid = col_match_uid[i]

        # Guard against None match_uid — would cause cache collisions
        if match_uid is None:
            logger.warning("Skipping row with None match_uid: %s", col_player_id[i])
            # `cols`, not ALL_RATING_COLUMNS: under stamp the output carries the
            # serve-count columns too, and padding only the base set would leave
            # those four lists one short of every other.
            for col in cols:
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
            elo_ratings[player_id] = initialize_player(ranking, svc_cfg)
            # Seed Glicko mu from the rank-based Elo seed (same Elo-point scale),
            # not flat INITIAL_MU — flat seeding overrates weak entrants and is a
            # primary driver of mu inflation.
            glicko_ratings[player_id] = GlickoRating(mu=elo_ratings[player_id].elo)
        if opp_id not in elo_ratings:
            opp_ranking = col_opp_rank[i]
            elo_ratings[opp_id] = initialize_player(opp_ranking, svc_cfg)
            glicko_ratings[opp_id] = GlickoRating(mu=elo_ratings[opp_id].elo)
        if mov_tracker is not None:
            # Same rank-based seed as the base rating, so a variant's early
            # divergence from elo is pure mechanism, never seeding.
            mov_tracker.ensure_player(player_id, elo_ratings[player_id].elo)
            mov_tracker.ensure_player(opp_id, elo_ratings[opp_id].elo)

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
            if mov_tracker is not None:
                mov_tracker.append_output(
                    output, p_cached["mov"], o_cached["mov"]
                )
            continue

        # First row for this match - apply inactivity and cache pre-match values
        if isinstance(match_date, date):
            player_rating.rd = apply_inactivity_rd(
                player_rating.rd, player_rating.last_match_date, match_date
            )
            # Serve and return grow from their OWN clocks. Keyed to
            # last_match_date they would stop growing during a run of matches
            # that carry no serve stats — the rating stands still while its
            # stated uncertainty does too, which is the same lie as decaying it.
            player_rating.serve_rd = apply_inactivity_rd(
                player_rating.serve_rd,
                player_rating.last_serve_update_date, match_date,
            )
            player_rating.return_rd = apply_inactivity_rd(
                player_rating.return_rd,
                player_rating.last_return_update_date, match_date,
            )
            opp_rating.rd = apply_inactivity_rd(
                opp_rating.rd, opp_rating.last_match_date, match_date
            )
            opp_rating.serve_rd = apply_inactivity_rd(
                opp_rating.serve_rd,
                opp_rating.last_serve_update_date, match_date,
            )
            opp_rating.return_rd = apply_inactivity_rd(
                opp_rating.return_rd,
                opp_rating.last_return_update_date, match_date,
            )
            if mov_tracker is not None:
                mov_tracker.apply_inactivity(player_id, match_date)
                mov_tracker.apply_inactivity(opp_id, match_date)
            # Each surface/venue adjustment grows from its OWN clock too. A
            # player who has not been on grass for two years should show low
            # confidence in their grass adjustment however much they have played
            # elsewhere.
            for r in (player_rating, opp_rating):
                for ax in SERVE_AXES:
                    for side in ("svc", "ret"):
                        setattr(r, f"{side}_{ax}_rd", apply_inactivity_rd(
                            getattr(r, f"{side}_{ax}_rd"),
                            getattr(r, f"last_{side}_{ax}_date"), match_date,
                        ))

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
        if mov_tracker is not None:
            mov_p_vals = mov_tracker.capture(player_id)
            mov_o_vals = mov_tracker.capture(opp_id)
            match_ratings_cache[match_uid][player_id]["mov"] = mov_p_vals
            match_ratings_cache[match_uid][opp_id]["mov"] = mov_o_vals

        # Record PRE-MATCH values
        _append_ratings_to_output(
            output, elo_player, elo_opp, glicko_p_vals, glicko_o_vals,
        )
        if mov_tracker is not None:
            mov_tracker.append_output(output, mov_p_vals, mov_o_vals)

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

        # MOV variants: own bare state, own K schedule inputs, own reversion —
        # all inside the tracker. Games from THIS row's orientation; the
        # incomplete guard falls back to the binary update.
        if mov_tracker is not None:
            p_games = sum(
                g[i] for g in col_player_set_games if g[i] is not None
            )
            o_games = sum(
                g[i] for g in col_opp_set_games if g[i] is not None
            )
            mov_tracker.update_match(
                player_id, opp_id, bool(won), round_name, tournament_level,
                p_games, o_games,
                margin_is_valid(
                    p_games + o_games, col_reason[i], col_result_type[i]
                ),
                match_date if isinstance(match_date, date) else None,
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

        # Update serve/return Elo — two sub-games per match.
        #
        # K comes from the serve and return dimensions' OWN rd and experience,
        # not the base rating's. A player with 200 career matches but few
        # carrying serve stats has a wide, honest serve_rd; deriving K from
        # `match_count` would update them at an established player's rate and
        # make that honesty decorative.
        #
        # Zero-sum survives because it comes from passing ONE k to
        # update_serve_elo, not from the two sides having equal K. Averaging the
        # two sides is what the previous code already did one level up.

        # Which adjustment axes these conditions train. A function of the
        # match, not of the player, so it is resolved once.
        axes = player_rating.serve_axes(surface, indoor)

        # Sub-game 1: player serves, opponent returns
        serve_won = col_pts_service_pts_won[i]
        serve_played = col_pts_service_pts_played[i]
        player_serve_pct = None
        if serve_won is not None and serve_played and serve_played > 0:
            player_serve_pct = serve_won / serve_played

        k_sub1 = (
            k_factor_from(
                player_rating.serve_rd, player_rating.serve_match_count,
                round_name, tournament_level,
            )
            + k_factor_from(
                opp_rating.return_rd, opp_rating.return_match_count,
                round_name, tournament_level,
            )
        ) / 2 * svc_cfg.k_mult

        # Expectation uses EFFECTIVE ratings (base plus whichever surface/venue
        # adjustments these conditions engage); the delta then lands on the base
        # and on each engaged adjustment at its own step size. Same split as
        # update_elo, which takes an effective rating but returns a base delta.
        surprise1 = serve_surprise(
            player_serve_pct, surface,
            player_rating.effective_serve_elo(surface, indoor),
            opp_rating.effective_return_elo(surface, indoor),
            svc_cfg, indoor,
        )
        if surprise1 is not None:
            player_rating.serve_elo += k_sub1 * surprise1
            opp_rating.return_elo -= k_sub1 * surprise1
            k_adj1 = k_sub1 * SERVE_SURFACE_K_MULT
            for ax in axes:
                _bump(player_rating, f"svc_{ax}_adj", k_adj1 * surprise1)
                _bump(opp_rating, f"ret_{ax}_adj", -k_adj1 * surprise1)
            player_rating.serve_match_count += 1
            opp_rating.return_match_count += 1
            if isinstance(match_date, date):
                player_rating.last_serve_update_date = match_date
                opp_rating.last_return_update_date = match_date
                for ax in axes:
                    setattr(player_rating, f"last_svc_{ax}_date", match_date)
                    setattr(opp_rating, f"last_ret_{ax}_date", match_date)

        # Sub-game 2: opponent serves, player returns
        opp_serve_won = col_opp_pts_service_pts_won[i]
        opp_serve_played = col_opp_pts_service_pts_played[i]
        opp_serve_pct = None
        if opp_serve_won is not None and opp_serve_played and opp_serve_played > 0:
            opp_serve_pct = opp_serve_won / opp_serve_played

        k_sub2 = (
            k_factor_from(
                opp_rating.serve_rd, opp_rating.serve_match_count,
                round_name, tournament_level,
            )
            + k_factor_from(
                player_rating.return_rd, player_rating.return_match_count,
                round_name, tournament_level,
            )
        ) / 2 * svc_cfg.k_mult

        # No snapshot needed between the sub-games: this one reads the
        # opponent's SERVE side and the player's RETURN side, neither of which
        # sub-game 1 wrote.
        surprise2 = serve_surprise(
            opp_serve_pct, surface,
            opp_rating.effective_serve_elo(surface, indoor),
            player_rating.effective_return_elo(surface, indoor),
            svc_cfg, indoor,
        )
        if surprise2 is not None:
            opp_rating.serve_elo += k_sub2 * surprise2
            player_rating.return_elo -= k_sub2 * surprise2
            k_adj2 = k_sub2 * SERVE_SURFACE_K_MULT
            for ax in axes:
                _bump(opp_rating, f"svc_{ax}_adj", k_adj2 * surprise2)
                _bump(player_rating, f"ret_{ax}_adj", -k_adj2 * surprise2)
            opp_rating.serve_match_count += 1
            player_rating.return_match_count += 1
            if isinstance(match_date, date):
                opp_rating.last_serve_update_date = match_date
                player_rating.last_return_update_date = match_date
                for ax in axes:
                    setattr(opp_rating, f"last_svc_{ax}_date", match_date)
                    setattr(player_rating, f"last_ret_{ax}_date", match_date)

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

            # Serve and return drift too and had no reversion at all: mean
            # serve_elo climbed from 1500 to ~1718 by 2020. Scaled by each
            # dimension's OWN rd, and skipped entirely until that dimension has
            # landed at least one real update — otherwise reversion fires at
            # full strength on a freshly seeded rating whose rd is still at the
            # ceiling, erasing the seed before any serve data can test it.
            if r.serve_match_count > 0:
                svc_rev = REVERSION_RATE * (r.serve_rd / DEFAULT_RD)
                r.serve_elo += svc_rev * (DEFAULT_ELO - r.serve_elo)
            if r.return_match_count > 0:
                ret_rev = REVERSION_RATE * (r.return_rd / DEFAULT_RD)
                r.return_elo += ret_rev * (DEFAULT_ELO - r.return_elo)

        # Update RD (decreases after match). Serve and return decay only when
        # their own update actually landed — a match with no point-level serve
        # stats teaches those dimensions nothing, so reporting increased
        # confidence in them would be false.
        player_rating.rd = update_rd(player_rating.rd)
        opp_rating.rd = update_rd(opp_rating.rd)
        if player_serve_pct is not None:
            player_rating.serve_rd = update_rd(player_rating.serve_rd)
            opp_rating.return_rd = update_rd(opp_rating.return_rd)
        if opp_serve_pct is not None:
            opp_rating.serve_rd = update_rd(opp_rating.serve_rd)
            player_rating.return_rd = update_rd(player_rating.return_rd)
        # Adjustments revert toward zero on EXACTLY the same cadence their rd
        # decays: the axes this match trained, on the side whose sub-game
        # landed. Reverting more broadly than that is what suppressed thin
        # axes — grass is ~3% of matches, so an ungated reversion pulled a
        # grass adjustment toward zero on ~97% of the matches in between,
        # costing roughly 39% of its magnitude between consecutive grass
        # seasons ((1 - 0.005)^100).
        #
        # Base Elo reverts its surface adjustments on every match, which looks
        # like the same thing but is not: base Elo scales reversion by the
        # overall rd, and that rd decays every match too, so the strength and
        # the firing share one cadence. Pass two scales by a per-axis rd that
        # decays narrowly, so firing broadly leaves the two mismatched.
        #
        # A frozen adjustment does not lose its staleness signal: the axis's rd
        # keeps growing on its own clock while the player is away, so
        # confidence decays even though the value holds. Separating "what we
        # believe" from "how sure we are" is the point — letting confidence
        # decay leak into the belief is what this fixes.
        for ax in axes:
            for r, side, landed in (
                (player_rating, "svc", player_serve_pct),
                (opp_rating, "ret", player_serve_pct),
                (opp_rating, "svc", opp_serve_pct),
                (player_rating, "ret", opp_serve_pct),
            ):
                if landed is None:
                    continue
                rev = REVERSION_RATE * (getattr(r, f"{side}_{ax}_rd") / DEFAULT_RD)
                setattr(r, f"{side}_{ax}_adj",
                        getattr(r, f"{side}_{ax}_adj") * (1 - rev))

        # Adjustment rds decay only for the axes this match actually trained,
        # and only on the side whose sub-game landed.
        for ax in axes:
            if player_serve_pct is not None:
                setattr(player_rating, f"svc_{ax}_rd",
                        update_rd(getattr(player_rating, f"svc_{ax}_rd")))
                setattr(opp_rating, f"ret_{ax}_rd",
                        update_rd(getattr(opp_rating, f"ret_{ax}_rd")))
            if opp_serve_pct is not None:
                setattr(opp_rating, f"svc_{ax}_rd",
                        update_rd(getattr(opp_rating, f"svc_{ax}_rd")))
                setattr(player_rating, f"ret_{ax}_rd",
                        update_rd(getattr(player_rating, f"ret_{ax}_rd")))

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

    if stamp:
        # The values actually in force, read off the instance used — never
        # re-derived from the constants module, which would reintroduce the
        # ambiguity about which source was authoritative.
        df = df.with_columns([
            pl.lit(v).alias(f"_serve_cfg_{k}") for k, v in svc_cfg.stamp().items()
        ])

    logger.info(
        "Computed ratings for %d players across %d unique matches (%d rows)",
        len(elo_ratings),
        len(processed_matches),
        len(df),
    )
    return df
