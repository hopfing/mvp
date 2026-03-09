"""Tests for BetRivers odds scraper."""

import pytest

from mvp.betrivers.odds import _is_atp_challenger


class TestCircuitFiltering:
    def test_atp_included(self):
        assert _is_atp_challenger("atp") is True

    def test_challenger_included(self):
        assert _is_atp_challenger("challenger") is True

    def test_challenger_qual_included(self):
        assert _is_atp_challenger("challenger_qual_") is True

    def test_wta_excluded(self):
        assert _is_atp_challenger("wta") is False

    def test_wta_doubles_excluded(self):
        assert _is_atp_challenger("wta_doubles") is False

    def test_atp_doubles_excluded(self):
        assert _is_atp_challenger("atp_doubles") is False

    def test_itf_women_excluded(self):
        assert _is_atp_challenger("itf_women") is False

    def test_itf_women_qual_excluded(self):
        assert _is_atp_challenger("itf_women_qual_") is False

    def test_itf_men_qual_excluded(self):
        assert _is_atp_challenger("itf_men_qual_") is False

    def test_utr_excluded(self):
        assert _is_atp_challenger("utr_pro_tennis_series") is False

    def test_wta125_excluded(self):
        assert _is_atp_challenger("wta125") is False

    def test_unknown_excluded(self):
        assert _is_atp_challenger("some_new_category") is False
