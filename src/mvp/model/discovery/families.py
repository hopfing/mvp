"""Candidate-family enumeration for family-level forward selection.

Plan: mvp-docs/plans/2026-08-25-fs-protocol-redesign.md, design item 1.

The unit of selection is the family, not the column: a family is one base
statistic across its side (`player_`/`opp_`), window (`(days=N)`), and
combiner (`_diff`/`_matchup`, unprefixed `_sum`) forms. `surface_`- and
`tourn_`-conditioned variants keep their prefix and form separate families
(different conditioning).

The mechanical rule covers ~97% of the pool. The rest is a hand-maintained
overlay, decided from the 2026-08-25 family report (scripts/fs_family_report.py
over the stage1_lead_residual pool): combiner stems registered under a
different name than their base stat (`ret_pts_won_pct_matchup` vs
`pts_return_won_pct`), unprefixed context flags, symmetric cross-player
aggregates, and `_x_` interactions assigned to their primary component.
`group_candidates` returns unassigned leftovers rather than guessing, so a
pool change surfaces as a visible residue, not a silent misgrouping.
"""

from __future__ import annotations

import re
from collections import defaultdict

_WINDOW = re.compile(r"\(days=\d+\)$")
# model=<stem> parameterization (prior / chain_shape specs, reachable via
# features.add) strips like a window: the family is the transform output,
# not one garbled singleton per stem. Assumes model= is the spec's ONLY
# parenthesized param — true for every such transform today; a transform
# combining model= with a second param would need this (and discover.py's
# stem regex) generalized.
_MODEL_PARAM = re.compile(r"\(model=[^)]+\)$")

# Combiner-only stems -> the family their base stat actually lives under.
STEM_REMAP: dict[str, str] = {
    "ret_pts_won_pct": "pts_return_won_pct",
    "svc_pts_won_pct": "pts_service_won_pct",
    "surface_ret_pts_won_pct": "surface_pts_return_won_pct",
    "surface_svc_pts_won_pct": "surface_pts_service_won_pct",
    "tourn_ret_pts_won_pct": "tourn_pts_return_won_pct",
    "tourn_svc_pts_won_pct": "tourn_pts_service_won_pct",
    "glicko": "glicko_mu",
    "glicko_logistic": "glicko_mu",
    "svc_elo": "serve_elo",
    "ret_elo": "return_elo",
    "svc_clutch": "serve_clutch",
    "ret_clutch": "return_clutch",
    "elo_clutch": "overall_clutch",
    "elo_tb_clutch": "tb_clutch",
    "elo_indoor_adj": "indoor_adj",
    "svc_first_serve_power": "first_serve_power",
    "ret_ace_resistance": "ace_resistance",
    "svc_second_serve_reliability": "second_serve_reliability",
    "ret_bp_pct": "ret_bp_convert_pct",
    "svc_bp_pct": "svc_bp_save_pct",
    "surface_bp_pct": "surface_bp_save_pct",
    "surface_ret_bp_pct": "surface_ret_bp_convert_pct",
    "tourn_ret_bp_pct": "tourn_ret_bp_convert_pct",
    "tourn_svc_bp_pct": "tourn_svc_bp_save_pct",
    "tourn_hold_vs_break": "tourn_hold_pct",
    "tourn_break_vs_hold": "tourn_break_pct",
}

# Unprefixed / interaction stems -> hand-assigned family.
MANUAL_FAMILIES: dict[str, str] = {
    # tournament tier context
    **{s: "ctx_tier" for s in (
        "is_atp_1000", "is_atp_500", "is_atp_250", "is_chal", "is_tour",
        "is_futures", "is_itf", "is_grand_slam", "is_qualifying",
        "is_challenger_any", "is_challenger_high", "is_challenger_low",
        "is_challenger_50", "is_challenger_75", "is_challenger_100",
        "is_challenger_125", "is_challenger_175",
        "tournament_tier_ordinal", "prize_money_log",
    )},
    # surface / venue flags
    **{s: "ctx_surface" for s in ("is_clay", "is_grass", "is_hard", "is_indoor")},
    # calendar position
    **{s: "ctx_calendar" for s in (
        "match_season", "match_season_qtr", "match_period", "match_period_qtr",
    )},
    # round position and format
    **{s: "ctx_round" for s in (
        "round_ordinal", "tournament_round_ordinal", "is_draw_opener",
    )},
    "best_of": "ctx_format",
    # cross-player style pairing flags
    "matchup_both_counterpunchers": "style_pair_flags",
    "matchup_both_power_servers": "style_pair_flags",
    # symmetric closeness / uncertainty aggregates
    **{s: "elo_closeness" for s in (
        "elo_avg", "elo_avg_sq", "elo_min",
        "elo_surface_diff_abs", "elo_surface_diff_sq",
        "elo_diff_x_elo_avg", "elo_diff_x_rd_sum",
        "elo_surface_diff_x_level_disp", "elo_surface_indoor_diff_x_level_disp",
    )},
    **{s: "glicko_uncertainty" for s in (
        "glicko_bhattacharyya_rd", "glicko_diff_abs", "glicko_diff_sq",
        "glicko_joint_rd", "glicko_joint_total", "glicko_overlap_coefficient_rd",
        "glicko_rd_max", "glicko_rd_min", "glicko_rd_ratio",
        "glicko_rd_x_days_since_last_match", "glicko_rd_x_match_count",
        "glicko_surface_rd_sum",
        "glicko_mu_diff_x_formvol_asymmetry", "glicko_mu_diff_x_rd_asymmetry",
    )},
    **{s: "level_gap_shape" for s in (
        "level_gap_disp", "level_gap_minabs", "level_gap_prod",
        "level_gap_pts_disp", "level_gap_pts_minabs", "level_gap_pts_prod",
    )},
    "match_count_max": "match_count",
    # match-level IID projections
    "iid_expected_games_per_set": "iid_match_level",
    "iid_tiebreak_prob": "iid_match_level",
    # prefixed glicko level x uncertainty interactions -> primary component
    "glicko_mu_diff_x_opp_formvol": "glicko_mu",
    "glicko_mu_diff_x_player_formvol": "glicko_mu",
    "glicko_mu_diff_x_opp_rd": "glicko_mu",
    "glicko_mu_diff_x_player_rd": "glicko_mu",
    "glicko_mu_x_rd": "glicko_mu",
    "glicko_diff_x_rd_sum": "glicko_mu",
}


def family_of(name: str) -> str | None:
    """Family key for one candidate column name, or None if unassignable."""
    base = _WINDOW.sub("", name)
    base = _MODEL_PARAM.sub("", base)
    side = None
    for p in ("player_", "opp_"):
        if base.startswith(p):
            side, base = p, base[len(p):]
            break
    if base in MANUAL_FAMILIES:
        return MANUAL_FAMILIES[base]
    if side is not None:
        if "_x_" in base:
            return None  # prefixed interaction not in the overlay
        for suf in ("_diff", "_matchup"):
            if base.endswith(suf):
                base = base[: -len(suf)]
                break
        return STEM_REMAP.get(base, base)
    # unprefixed: `<stem>_sum` cross-player combiner joins the stem's family
    if base.endswith("_sum"):
        return STEM_REMAP.get(base[:-4], base[:-4])
    return None


def group_candidates(
    names: list[str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Group candidate columns into families.

    Returns (families, unassigned). `unassigned` must be inspected, not
    ignored: a non-empty residue means the pool grew a shape the rule and
    overlay don't cover yet.
    """
    families: dict[str, list[str]] = defaultdict(list)
    unassigned: list[str] = []
    for name in names:
        fam = family_of(name)
        if fam is None:
            unassigned.append(name)
        else:
            families[fam].append(name)
    return dict(families), unassigned
