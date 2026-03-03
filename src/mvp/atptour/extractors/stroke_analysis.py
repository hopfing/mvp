"""StrokeAnalysisExtractor - wrapper for MatchCentreExtractor."""

from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor


class StrokeAnalysisExtractor(MatchCentreExtractor):
    """Fetch stroke analysis data from Infosys API.

    Thin wrapper around MatchCentreExtractor for standalone use.
    """

    def __init__(self, data_root=None):
        super().__init__(data_root=data_root, data_types=[DataType.STROKE_ANALYSIS])
