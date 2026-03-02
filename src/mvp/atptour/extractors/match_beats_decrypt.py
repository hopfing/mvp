"""MatchBeats decryption utilities.

Handles AES-CBC decryption of MatchBeats API responses.
Key is derived from the lastModified timestamp in the response.
"""

from datetime import UTC, datetime


def _int_to_base36(n: int) -> str:
    """Convert integer to base36 string."""
    if n == 0:
        return "0"
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    result = []
    while n:
        result.append(digits[n % 36])
        n //= 36
    return "".join(reversed(result))


def _int_to_base24(n: int) -> str:
    """Convert integer to base24 string."""
    if n == 0:
        return "0"
    digits = "0123456789abcdefghijklmn"
    result = []
    while n:
        result.append(digits[n % 24])
        n //= 24
    return "".join(reversed(result))


def derive_key(last_modified_ms: int) -> str:
    """Derive 16-char AES key from lastModified timestamp.

    Algorithm reverse-engineered from ATP MatchBeats JavaScript.

    Args:
        last_modified_ms: Unix timestamp in milliseconds

    Returns:
        16-character key string: # + 14 chars + $
    """
    dt = datetime.fromtimestamp(last_modified_ms / 1000, tz=UTC)
    day = dt.day
    year = dt.year

    # Part 1: timestamp as hex, interpreted as decimal, then base36
    part1 = _int_to_base36(int(str(int(last_modified_ms)), 16))

    # Part 2: year/day calculation in base24
    year_reversed = int(str(year)[::-1])
    day_reversed = int(f"{day:02d}"[::-1])
    part2 = _int_to_base24((year + year_reversed) * (day + day_reversed))

    # Combine and pad/truncate to 14 chars
    combined = part1 + part2
    if len(combined) < 14:
        combined = combined.ljust(14, "0")
    else:
        combined = combined[:14]

    return "#" + combined + "$"
