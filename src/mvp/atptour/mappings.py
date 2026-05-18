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

    Tries enum member name first (e.g., "QF" -> Round.QF, case-insensitive),
    then falls back to display-name lookup via ROUND_NORMALIZATION.
    Strips "Day N" suffixes (e.g., "Round Robin Day 2" -> "Round Robin")
    before lookup. Raises ValueError on unmapped round names — this is
    intentional fail-hard behavior per ADR-002.
    """
    cleaned = _DAY_SUFFIX_RE.sub("", raw.strip())
    # Try enum member name first (e.g., "QF", "R16", "BRONZE")
    try:
        return Round[cleaned.upper()]
    except KeyError:
        pass
    # Try display name (e.g., "Quarterfinals", "Round of 16")
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
    "SR:COMPETITOR:42152": "N679",
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


_PLACEHOLDER_IDS = frozenset({
    "0", "AAA1", "AAA2", "AAA3", "AAA4", "AAA5", "AAA6", "AAA7", "AAA8",
    "X500", "X501", "X502", "X503", "X504", "X505", "X506", "X507", "X508",
    "X509", "X510", "X511", "X512", "X513", "X514", "X515", "X516", "X517",
    "X519", "X521", "X526", "X577", "X578", "X581",
})


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


MATCH_UID_PATTERN = re.compile(
    r"^\d{4}_\d+_(?:SGL|DBL)_[A-Z0-9]+_(?:[A-Z0-9]+_){1,3}[A-Z0-9]+$"
)


def create_match_uid(
    year: int,
    tournament_id: str,
    round: Round,
    player_ids: list[str],
    is_doubles: bool,
) -> str:
    """Create stable match UID for joining across datasets."""
    draw = "DBL" if is_doubles else "SGL"
    sorted_ids = "_".join(sorted(pid.upper() for pid in player_ids))
    match_uid = f"{year}_{tournament_id}_{draw}_{round.value}_{sorted_ids}"
    if not MATCH_UID_PATTERN.match(match_uid):
        bad_ids = [pid for pid in player_ids if ":" in pid]
        hint = (
            f" Player ID(s) {bad_ids} look like Sportradar format — "
            f"add mapping to SR_ID_MAPPING in mappings.py."
            if bad_ids
            else ""
        )
        raise ValueError(f"Generated invalid match_uid: {match_uid}.{hint}")
    return match_uid


def parse_seed_entry(value: str | None) -> tuple[int | None, str | None]:
    """Parse combined seed/entry text into (seed, entry) tuple."""
    if not value:
        return None, None
    value = value.strip("()")
    if not value:
        return None, None
    if "/" in value:
        parts = value.split("/", 1)
        try:
            return int(parts[0]), parts[1] or None
        except ValueError:
            return None, value
    try:
        return int(value), None
    except ValueError:
        return None, value
