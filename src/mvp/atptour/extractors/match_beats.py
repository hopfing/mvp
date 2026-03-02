"""MatchBeatsExtractor - backwards-compatible wrapper for MatchCentreExtractor."""

from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor


class MatchBeatsExtractor(MatchCentreExtractor):
    """Fetch MatchBeats point-by-point data from Infosys API.

    This is a thin wrapper around MatchCentreExtractor for backwards
    compatibility. New code should use MatchCentreExtractor directly.
    """

    def __init__(self, data_root=None):
        super().__init__(data_root=data_root, data_types=[DataType.MATCH_BEATS])
