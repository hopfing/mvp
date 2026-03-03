"""Tests for StrokeAnalysisExtractor."""

from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor
from mvp.atptour.extractors.stroke_analysis import StrokeAnalysisExtractor


class TestStrokeAnalysisExtractor:
    """Tests for StrokeAnalysisExtractor."""

    def test_extractor_uses_stroke_analysis_data_type(self, tmp_path):
        """Should configure MatchCentreExtractor with STROKE_ANALYSIS only."""
        extractor = StrokeAnalysisExtractor(data_root=tmp_path)
        assert extractor.data_types == [DataType.STROKE_ANALYSIS]

    def test_inherits_from_match_centre_extractor(self, tmp_path):
        """Should be a MatchCentreExtractor subclass."""
        extractor = StrokeAnalysisExtractor(data_root=tmp_path)
        assert isinstance(extractor, MatchCentreExtractor)
