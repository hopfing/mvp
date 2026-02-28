"""Feature Engine for computing features from matches data."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def parse_feature_spec(spec: str) -> tuple[str, dict[str, Any]]:
    """Parse a feature specification string into name and parameters.

    Args:
        spec: Feature spec like "win_rate" or "win_rate(days=30)".

    Returns:
        Tuple of (feature_name, params_dict).

    Raises:
        ValueError: If the spec is malformed.

    Examples:
        >>> parse_feature_spec("win_rate")
        ("win_rate", {})
        >>> parse_feature_spec("win_rate(days=30)")
        ("win_rate", {"days": 30})
    """
    spec = spec.strip()

    # Simple case: no parameters
    if "(" not in spec:
        return spec, {}

    # Extract name and params
    match = re.match(r"^(\w+)\((.+)\)$", spec)
    if not match:
        raise ValueError(f"Invalid feature spec: {spec}")

    name = match.group(1)
    params_str = match.group(2)

    # Parse parameters
    params: dict[str, Any] = {}
    for param in params_str.split(","):
        param = param.strip()
        if "=" not in param:
            raise ValueError(f"Invalid feature spec: {spec}")

        key, value = param.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key or not value:
            raise ValueError(f"Invalid feature spec: {spec}")

        # Parse value type
        params[key] = _parse_value(value)

    return name, params


def _parse_value(value: str) -> Any:
    """Parse a parameter value string into its appropriate type."""
    # String with quotes
    if (value.startswith("'") and value.endswith("'")) or (
        value.startswith('"') and value.endswith('"')
    ):
        return value[1:-1]

    # Boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False

    # Integer
    try:
        return int(value)
    except ValueError:
        pass

    # Float
    try:
        return float(value)
    except ValueError:
        pass

    # Return as string if nothing else matches
    return value


class FeatureEngine:
    """Engine for computing features from matches.parquet.

    The engine loads match data, computes requested features using registered
    feature functions, and caches results for reuse.
    """

    def __init__(self, matches_path: Path, cache_dir: Path) -> None:
        """Initialize the Feature Engine.

        Args:
            matches_path: Path to the matches.parquet file.
            cache_dir: Directory for caching computed features.
        """
        self.matches_path = matches_path
        self.cache_dir = cache_dir

        # Create cache directory if it doesn't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
