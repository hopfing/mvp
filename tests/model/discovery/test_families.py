"""Tests for candidate-family enumeration."""

from mvp.model.discovery.families import family_of, group_candidates


class TestFamilyRule:
    def test_side_window_and_combiners_collapse_to_one_family(self):
        assert family_of("player_hold_pct(days=365)") == "hold_pct"
        assert family_of("opp_hold_pct") == "hold_pct"
        assert family_of("player_hold_pct_diff") == "hold_pct"
        assert family_of("hold_pct_sum(days=730)") == "hold_pct"

    def test_surface_conditioning_is_a_separate_family(self):
        assert family_of("player_surface_hold_pct(days=180)") == "surface_hold_pct"
        assert family_of("player_hold_pct") == "hold_pct"

    def test_combiner_only_stem_remaps_to_base_family(self):
        # matchup registered under a different stem than its base stat
        matchup = family_of("player_ret_pts_won_pct_matchup(days=30)")
        assert matchup == "pts_return_won_pct"
        assert family_of("player_pts_return_won_pct(days=30)") == "pts_return_won_pct"
        svc = family_of("player_svc_elo_surface_indoor_matchup")
        assert svc == "svc_elo_surface_indoor"
        assert family_of("player_glicko_diff") == "glicko_mu"

    def test_manual_overlay_context_and_aggregates(self):
        assert family_of("is_grand_slam") == "ctx_tier"
        assert family_of("is_indoor") == "ctx_surface"
        assert family_of("elo_avg_sq") == "elo_closeness"
        assert family_of("glicko_rd_ratio") == "glicko_uncertainty"
        assert family_of("player_glicko_mu_diff_x_opp_rd") == "glicko_mu"
        assert family_of("match_count_max(days=365)") == "match_count"

    def test_unknown_shapes_are_unassigned_not_guessed(self):
        assert family_of("some_new_symmetric_thing") is None
        assert family_of("player_elo_diff_x_new_interaction") is None

    def test_group_candidates_returns_residue(self):
        fams, unassigned = group_candidates([
            "player_hold_pct", "opp_hold_pct(days=30)", "mystery_column",
        ])
        assert fams == {"hold_pct": ["player_hold_pct", "opp_hold_pct(days=30)"]}
        assert unassigned == ["mystery_column"]
