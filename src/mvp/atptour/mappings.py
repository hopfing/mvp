"""ATP-specific mapping tables and normalization functions.

Used by ATP parsers and schemas to normalize raw values from atptour.com
into canonical forms during staging.
"""

import re

from mvp.common.enums import Round

ROUND_NORMALIZATION: dict[str, Round] = {
    "Final": Round.F,
    "Finals": Round.F,
    "Semifinals": Round.SF,
    "Semi-Finals": Round.SF,
    "Semifinal": Round.SF,
    "Quarterfinals": Round.QF,
    "Quarter-Finals": Round.QF,
    "Quarterfinal": Round.QF,
    "Round of 16": Round.R16,
    "Round of 32": Round.R32,
    "Round of 64": Round.R64,
    "Round of 128": Round.R128,
    "Round Robin": Round.RR,
    "1st Round Qualifying": Round.Q1,
    "2nd Round Qualifying": Round.Q2,
    "3rd Round Qualifying": Round.Q3,
    "Bronze Medal Match": Round.THIRDPLACE,
    "Olympic Bronze": Round.THIRDPLACE,
    "Third Place": Round.THIRDPLACE,
    "3rd/4th": Round.THIRDPLACE,
    "3rd/4th Place Match": Round.THIRDPLACE,
    "Host City Finals": Round.HCF,
}

_DAY_SUFFIX_RE = re.compile(r"\s+Day\s+\d+$", re.IGNORECASE)


def normalize_round(raw: str) -> Round:
    """Normalize a raw round name string to a canonical Round enum value.

    Strips "Day N" suffixes (e.g., "Round Robin Day 2" -> "Round Robin")
    before lookup. Raises ValueError on unmapped round names — this is
    intentional fail-hard behavior per ADR-002.
    """
    cleaned = _DAY_SUFFIX_RE.sub("", raw.strip())
    result = ROUND_NORMALIZATION.get(cleaned)
    if result is None:
        raise ValueError(
            f"Unmapped round name: '{raw}' (cleaned: '{cleaned}'). "
            f"Add it to ROUND_NORMALIZATION in mappings.py."
        )
    return result


# Mapping Sportradar player IDs to ATP IDs, partial mapping based on observed data.
SR_ID_MAPPING: dict[str, str] = {
    "SR:COMPETITOR:972327": "J0DZ",
    "SR:COMPETITOR:1055851": "H0K0",
    "SR:COMPETITOR:59700": "I326",
    "SR:COMPETITOR:145936": "KH77",
    "SR:COMPETITOR:637610": "O0BI",
    "SR:COMPETITOR:1021133": "M0UR",
    "SR:COMPETITOR:915589": "V0GR",
    "SR:COMPETITOR:915951": "R0IL",
    "SR:COMPETITOR:617530": "W0BU",
    "SR:COMPETITOR:168420": "AG08",
    "SR:COMPETITOR:634350": "D0HJ",
    "SR:COMPETITOR:948185": "M0SE",
    "SR:COMPETITOR:978611": "I0BF",
    "SR:COMPETITOR:235902": "F09U",
    "SR:COMPETITOR:1202375": "C0S3",
    "SR:COMPETITOR:16992": "W521",
}

_SR_PREFIX = "SR:COMPETITOR:"


def map_player_id(raw: str) -> str:
    """Map a raw player ID to a normalized uppercase ATP code.

    Normal ATP IDs (e.g., "s0ag" -> "S0AG") are uppercased.
    Sportradar-format IDs ("SR:COMPETITOR:*") are mapped via the lookup table.
    Raises ValueError on unmapped SR IDs — this is intentional fail-hard
    behavior per ADR-002.
    """
    upper = raw.upper()
    if not upper.startswith(_SR_PREFIX):
        return upper
    result = SR_ID_MAPPING.get(upper)
    if result is None:
        raise ValueError(
            f"Unmapped Sportradar player ID: '{raw}'. "
            f"Add the mapping to SR_ID_MAPPING in mappings.py."
        )
    return result


_PLACEHOLDER_IDS = frozenset(
    {"0", "AAA1", "AAA2", "AAA3", "AAA4", "AAA5", "AAA6", "AAA7", "AAA8"}
)


def is_placeholder_id(player_id: str) -> bool:
    """Return True if the player ID is a placeholder (bye / TBD)."""
    return player_id in _PLACEHOLDER_IDS


def normalize_flag_url(href: str) -> str:
    """Extract country code from a flag SVG URL.

    Example: "flags.svg#flag-ita" -> "ita"

    Raises ValueError if the expected pattern is not found.
    """
    marker = "#flag-"
    idx = href.find(marker)
    if idx == -1:
        raise ValueError(f"Flag URL does not contain '{marker}': '{href}'")
    return href[idx + len(marker) :]


def parse_duration(raw: str) -> int:
    """Parse a duration string into total seconds.

    Accepts "HH:MM" or "HH:MM:SS" format.

    Examples:
        "03:44"    -> 13440  (3 hours, 44 minutes)
        "02:50:39" -> 10239  (2 hours, 50 minutes, 39 seconds)
        "00:00"    -> 0

    Raises ValueError on unrecognized formats.
    """
    if not raw or not raw.strip():
        raise ValueError(f"Empty duration string: '{raw}'")

    parts = raw.strip().split(":")
    if len(parts) == 2:
        try:
            hours, minutes = int(parts[0]), int(parts[1])
        except ValueError:
            raise ValueError(f"Non-numeric duration components in '{raw}'")
        return hours * 3600 + minutes * 60
    if len(parts) == 3:
        try:
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
        except ValueError:
            raise ValueError(f"Non-numeric duration components in '{raw}'")
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f"Expected HH:MM or HH:MM:SS duration format, got '{raw}'")
