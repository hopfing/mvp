"""RallyAnalysisExtractor - wrapper for MatchCentreExtractor."""

from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor


class RallyAnalysisExtractor(MatchCentreExtractor):
    """Fetch rally analysis data from Infosys API.

    Thin wrapper around MatchCentreExtractor for standalone use.
    """

    def __init__(self, data_root=None):
        super().__init__(data_root=data_root, data_types=[DataType.RALLY_ANALYSIS])
