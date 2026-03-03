"""Tests for MatchBeatsAggregator."""

import polars as pl
import pytest

from mvp.atptour.aggregators.match_beats import MatchBeatsAggregator


class TestMatchBeatsAggregator:
    """Tests for MatchBeatsAggregator."""

    def test_aggregator_initializes(self, tmp_path):
        """Should initialize with data_root."""
        agg = MatchBeatsAggregator(data_root=tmp_path)
        assert agg.data_root == tmp_path
