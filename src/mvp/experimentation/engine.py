"""Feature Engine for computing features from matches data."""

from __future__ import annotations

import hashlib
import json
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
        self._manifest_path = self.cache_dir / "manifest.json"
        self._manifest: dict[str, Any] = self._load_manifest()

    def _load_manifest(self) -> dict[str, Any]:
        """Load the cache manifest from disk."""
        if self._manifest_path.exists():
            with open(self._manifest_path) as f:
                return json.load(f)
        return {"matches_hash": None, "features": {}}

    def _save_manifest(self) -> None:
        """Save the cache manifest to disk."""
        with open(self._manifest_path, "w") as f:
            json.dump(self._manifest, f, indent=2)

    def _compute_matches_hash(self) -> str:
        """Compute a hash of the matches file for cache invalidation."""
        # Use file size and modification time for quick hash
        stat = self.matches_path.stat()
        content = f"{stat.st_size}:{stat.st_mtime}"
        return hashlib.md5(content.encode()).hexdigest()

    def _get_cache_path(self, spec: str) -> Path:
        """Get the cache file path for a feature spec."""
        # Create a safe filename from the spec
        safe_name = re.sub(r"[^\w]", "_", spec)
        return self.cache_dir / f"{safe_name}.parquet"

    def _is_cached(self, spec: str, matches_hash: str) -> bool:
        """Check if a feature is cached and valid."""
        if self._manifest.get("matches_hash") != matches_hash:
            return False
        if spec not in self._manifest.get("features", {}):
            return False
        cache_path = self._get_cache_path(spec)
        return cache_path.exists()

    def _load_cached_feature(self, spec: str) -> pl.DataFrame:
        """Load a cached feature from disk."""
        cache_path = self._get_cache_path(spec)
        return pl.read_parquet(cache_path)

    def _cache_feature(
        self, spec: str, df: pl.DataFrame, columns: list[str], matches_hash: str
    ) -> None:
        """Cache a computed feature to disk."""
        cache_path = self._get_cache_path(spec)
        # Save only the key columns and feature columns
        key_cols = ["match_uid", "player_id"]
        df.select(key_cols + columns).write_parquet(cache_path)

        # Update manifest
        self._manifest["matches_hash"] = matches_hash
        if "features" not in self._manifest:
            self._manifest["features"] = {}
        self._manifest["features"][spec] = {
            "columns": columns,
            "path": str(cache_path),
        }
        self._save_manifest()

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

        # Check cache validity
        matches_hash = self._compute_matches_hash()

        # Track columns to mirror
        columns_to_mirror: list[str] = []

        # Compute each feature
        for spec in feature_specs:
            name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(name)

            # Build column name from params
            col_name = self._build_column_name(name, params)
            player_col = f"player_{col_name}"

            # Check cache first
            if self._is_cached(spec, matches_hash):
                # Load from cache and join to df
                cached = self._load_cached_feature(spec)
                df = df.join(
                    cached,
                    on=["match_uid", "player_id"],
                    how="left",
                )
            else:
                # Compute the feature expression
                expr = feature_def.func(**params)

                # Add to DataFrame with player_ prefix
                df = df.with_columns(expr.alias(player_col))

                # Cache the computed feature
                feature_columns = [player_col]
                self._cache_feature(spec, df, feature_columns, matches_hash)

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
