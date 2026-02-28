"""Feature Engine for computing features from matches data."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import polars as pl

from mvp.experimentation.registry import get_registry


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

        self._registry = get_registry()

    def compute(self, feature_specs: list[str]) -> pl.DataFrame:
        """Compute features for the given feature specifications.

        Args:
            feature_specs: List of feature specs like ["win_rate(days=30)"].

        Returns:
            DataFrame with match data and computed feature columns.
            Feature columns are prefixed with "player_" and suffixed with
            parameter values (e.g., "player_win_rate_30d").
            Features with mirror=True also get "opp_*" columns via self-join.
        """
        # Load matches data
        df = pl.read_parquet(self.matches_path)

        # Track columns to mirror
        columns_to_mirror: list[str] = []

        # Compute each feature
        for spec in feature_specs:
            name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(name)

            # Build column name from params
            col_name = self._build_column_name(name, params)
            player_col = f"player_{col_name}"

            # Compute the feature expression
            expr = feature_def.func(**params)

            # Add to DataFrame with player_ prefix
            df = df.with_columns(expr.alias(player_col))

            # Track for mirroring if enabled
            if feature_def.mirror:
                columns_to_mirror.append(player_col)

        # Mirror columns to create opp_* versions
        if columns_to_mirror:
            df = self._mirror_features(df, columns_to_mirror)

        return df

    def _mirror_features(
        self, df: pl.DataFrame, player_columns: list[str]
    ) -> pl.DataFrame:
        """Create opp_* columns by self-joining on match_uid.

        For each player_* column, creates an opp_* column containing the
        opponent's value of that feature within the same match.

        Args:
            df: DataFrame with player_* feature columns.
            player_columns: List of player_* column names to mirror.

        Returns:
            DataFrame with additional opp_* columns.
        """
        # Create lookup table: for each (match_uid, player_id), get the feature values
        # We'll join this back using opp_id to get opponent's features
        lookup_cols = ["match_uid", "player_id"] + player_columns
        lookup = df.select(lookup_cols)

        # Rename player_* to opp_* and player_id to a join key
        rename_map = {col: f"opp_{col[7:]}" for col in player_columns}
        rename_map["player_id"] = "_opp_lookup_id"
        lookup = lookup.rename(rename_map)

        # Join on match_uid and opp_id = _opp_lookup_id
        df = df.join(
            lookup,
            left_on=["match_uid", "opp_id"],
            right_on=["match_uid", "_opp_lookup_id"],
            how="left",
        )

        return df

    def _build_column_name(self, name: str, params: dict[str, Any]) -> str:
        """Build column name from feature name and parameters.

        Examples:
            win_rate + {days: 30} -> "win_rate_30d"
            h2h_wins + {days: 90} -> "h2h_wins_90d"
        """
        if not params:
            return name

        # For now, support "days" parameter with "d" suffix
        if "days" in params:
            return f"{name}_{params['days']}d"

        # Generic fallback: join param values with underscores
        suffix = "_".join(str(v) for v in params.values())
        return f"{name}_{suffix}"
