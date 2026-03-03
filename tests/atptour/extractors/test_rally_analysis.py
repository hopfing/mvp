"""Tests for RallyAnalysisExtractor."""

from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor
from mvp.atptour.extractors.rally_analysis import RallyAnalysisExtractor


class TestRallyAnalysisExtractor:
    """Tests for RallyAnalysisExtractor."""

    def test_extractor_uses_rally_analysis_data_type(self, tmp_path):
        """Should configure MatchCentreExtractor with RALLY_ANALYSIS only."""
        extractor = RallyAnalysisExtractor(data_root=tmp_path)
        assert extractor.data_types == [DataType.RALLY_ANALYSIS]

    def test_inherits_from_match_centre_extractor(self, tmp_path):
        """Should be a MatchCentreExtractor subclass."""
        extractor = RallyAnalysisExtractor(data_root=tmp_path)
        assert isinstance(extractor, MatchCentreExtractor)
