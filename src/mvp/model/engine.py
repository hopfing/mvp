"""Feature Engine for computing features from matches data."""


import hashlib
import inspect
import json
import re
from pathlib import Path
from typing import Any

import polars as pl

import mvp.model.features  # noqa: F401 - triggers feature registration
from mvp.model.registry import get_registry


def parse_feature_spec(spec: str) -> tuple[str | None, str, str, dict[str, Any]]:
    """Parse a feature specification string into prefix, base name, full name, and parameters.

    Args:
        spec: Feature spec like "player_win_rate(days=30)", "opp_win_rate(days=30)",
              or "is_clay" (for match-level features).

    Returns:
        Tuple of (prefix, base_name, full_name, params_dict).
        - prefix: "player", "opp", or None (for match-level features)
        - base_name: Feature name without prefix (e.g., "win_rate")
        - full_name: Full feature name (e.g., "player_win_rate" or "is_clay")
        - params_dict: Parameter dictionary

    Raises:
        ValueError: If the spec is malformed.

    Examples:
        >>> parse_feature_spec("player_win_rate(days=30)")
        ("player", "win_rate", "player_win_rate", {"days": 30})
        >>> parse_feature_spec("opp_win_rate(days=30)")
        ("opp", "win_rate", "opp_win_rate", {"days": 30})
        >>> parse_feature_spec("is_clay")
        (None, "is_clay", "is_clay", {})
    """
    spec = spec.strip()

    # Extract full name and params
    if "(" not in spec:
        full_name = spec
        params: dict[str, Any] = {}
    else:
        match = re.match(r"^(\w+)\((.+)\)$", spec)
        if not match:
            raise ValueError(f"Invalid feature spec: {spec}")

        full_name = match.group(1)
        params_str = match.group(2)

        # Parse parameters
        params = {}
        for param in params_str.split(","):
            param = param.strip()
            if "=" not in param:
                raise ValueError(f"Invalid feature spec: {spec}")

            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key or not value:
                raise ValueError(f"Invalid feature spec: {spec}")

            params[key] = _parse_value(value)

    # Extract prefix and base name
    if full_name.startswith("player_"):
        prefix: str | None = "player"
        base_name = full_name[7:]  # len("player_") = 7
    elif full_name.startswith("opp_"):
        prefix = "opp"
        base_name = full_name[4:]  # len("opp_") = 4
    else:
        # Match-level feature (no prefix)
        prefix = None
        base_name = full_name

    return prefix, base_name, full_name, params


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


def build_column_name(name: str, params: dict[str, Any]) -> str:
    """Build column name from feature name and parameters.

    Args:
        name: Feature name with prefix (e.g., "player_win_rate").
        params: Parameter dict (e.g., {"days": 30}).

    Returns:
        Column name (e.g., "player_win_rate_30d").

    Examples:
        >>> build_column_name("player_win_rate", {"days": 30})
        "player_win_rate_30d"
        >>> build_column_name("player_ranking_points_diff", {})
        "player_ranking_points_diff"
    """
    if not params:
        return name
    if "days" in params:
        return f"{name}_{params['days']}d"
    return f"{name}_{'_'.join(str(v) for v in params.values())}"


def get_feature_columns(feature_specs: list[str]) -> list[str]:
    """Get feature column names for a list of feature specs.

    Args:
        feature_specs: List of specs like ["player_win_rate(days=30)", "is_clay"].

    Returns:
        List of column names.
    """
    cols = []
    for spec in feature_specs:
        _prefix, _base, full_name, params = parse_feature_spec(spec)
        col_name = build_column_name(full_name, params)
        cols.append(col_name)
    return cols


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
        return {"cache_key": None, "features": {}}

    def _save_manifest(self) -> None:
        """Save the cache manifest to disk."""
        with open(self._manifest_path, "w") as f:
            json.dump(self._manifest, f, indent=2)

    def _compute_matches_hash(self) -> str:
        """Compute a hash of the matches file for cache invalidation."""
        stat = self.matches_path.stat()
        content = f"{stat.st_size}:{stat.st_mtime}"
        return hashlib.md5(content.encode()).hexdigest()

    def _compute_registry_hash(self) -> str:
        """Compute a hash of all feature function source code.

        Detects when feature implementations change so cached values
        from old code are not served.
        """
        sources = []
        for name in sorted(self._registry.list_features()):
            feat = self._registry.get(name)
            sources.append(f"{name}:{inspect.getsource(feat.func)}")
        return hashlib.md5("\n".join(sources).encode()).hexdigest()

    def _compute_cache_key(self) -> str:
        """Combined key: matches data + feature code."""
        return f"{self._compute_matches_hash()}:{self._compute_registry_hash()}"

    def _invalidate_cache(self) -> None:
        """Delete all cached feature files and reset the manifest."""
        for f in self.cache_dir.glob("*.parquet"):
            f.unlink()
        self._manifest = {"cache_key": None, "features": {}}
        self._save_manifest()

    def _get_cache_path(self, spec: str) -> Path:
        """Get the cache file path for a feature spec."""
        safe_name = re.sub(r"[^\w]", "_", spec)
        return self.cache_dir / f"{safe_name}.parquet"

    def _is_cached(self, spec: str, cache_key: str) -> bool:
        """Check if a feature is cached and valid."""
        if self._manifest.get("cache_key") != cache_key:
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
        self, spec: str, df: pl.DataFrame, columns: list[str], cache_key: str
    ) -> None:
        """Cache a computed feature to disk."""
        cache_path = self._get_cache_path(spec)
        key_cols = ["match_uid", "player_id"]
        df.select(key_cols + columns).write_parquet(cache_path)

        self._manifest["cache_key"] = cache_key
        if "features" not in self._manifest:
            self._manifest["features"] = {}
        self._manifest["features"][spec] = {
            "columns": columns,
            "path": str(cache_path),
        }
        self._save_manifest()

    def _resolve_dependencies(self, feature_specs: list[str]) -> list[str]:
        """Resolve feature dependencies and return ordered list.

        Ensures that if feature A depends on feature B, B is computed first.
        Dependencies inherit the same parameters (e.g., days) from the dependent.

        For diff-style features that need both player and opponent versions,
        dependencies are added for both prefixes.

        Args:
            feature_specs: List of feature specs.

        Returns:
            Ordered list with dependencies before dependents.
        """
        result = []
        seen = set()

        def add_with_deps(spec: str) -> None:
            if spec in seen:
                return
            seen.add(spec)

            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)

            # Recursively add dependencies first
            # Add BOTH player_ and opp_ versions since diff features need both
            for dep_name in feature_def.depends_on:
                for dep_prefix in ["player", "opp"]:
                    if params:
                        param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                        dep_spec = f"{dep_prefix}_{dep_name}({param_str})"
                    else:
                        dep_spec = f"{dep_prefix}_{dep_name}"
                    add_with_deps(dep_spec)

            # For opp_ specs of mirrored derived features, ensure the player_
            # version is also added — Phase 4 mirrors player_ → opp_
            if (
                prefix == "opp"
                and feature_def.mirror
                and feature_def.depends_on
            ):
                if params:
                    param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                    player_spec = f"player_{base_name}({param_str})"
                else:
                    player_spec = f"player_{base_name}"
                add_with_deps(player_spec)

            result.append(spec)

        for spec in feature_specs:
            add_with_deps(spec)

        return result

    def compute(self, feature_specs: list[str]) -> pl.DataFrame:
        """Compute features for the given feature specifications.

        Args:
            feature_specs: List of feature specs like ["player_win_rate(days=30)"].
                Specs can start with "player_", "opp_", or have no prefix (match-level).

        Returns:
            DataFrame with match data and computed feature columns.
        """
        # Validate: diff/matchup features must use player_ prefix
        for spec in feature_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            if prefix is None and (
                base_name.endswith("_diff") or base_name.endswith("_matchup")
            ):
                raise ValueError(
                    f"Feature '{spec}' requires a 'player_' prefix "
                    f"(use 'player_{spec}' instead)"
                )

        # Resolve dependencies first
        feature_specs = self._resolve_dependencies(feature_specs)

        # Separate features into categories:
        # - match_level_specs: no prefix, no depends_on (computed directly)
        # - match_level_derived_specs: no prefix, has depends_on (needs deps first)
        # - base_specs: player/opp prefixed, no depends_on
        # - derived_specs: player/opp prefixed, has depends_on
        match_level_specs = []
        match_level_derived_specs = []
        base_specs = []
        derived_specs = []
        for spec in feature_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)
            if prefix is None:
                if feature_def.depends_on:
                    match_level_derived_specs.append(spec)
                else:
                    match_level_specs.append(spec)
            elif feature_def.depends_on:
                derived_specs.append(spec)
            else:
                base_specs.append(spec)

        # Load matches data
        df = pl.read_parquet(self.matches_path)

        # Check cache validity — wipe everything if data or code changed
        cache_key = self._compute_cache_key()
        if self._manifest.get("cache_key") != cache_key:
            self._invalidate_cache()

        # Phase 0: compute match-level features (no prefix)
        computed_match_level: set[str] = set()
        for spec in match_level_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)

            if col_name not in computed_match_level:
                feature_def = self._registry.get(base_name)
                cache_spec = base_name
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"{base_name}({param_str})"

                if self._is_cached(cache_spec, cache_key):
                    cached = self._load_cached_feature(cache_spec)
                    df = df.join(
                        cached,
                        on=["match_uid", "player_id"],
                        how="left",
                    )
                else:
                    expr = feature_def.func(**params)
                    df = df.with_columns(expr.alias(col_name))
                    self._cache_feature(cache_spec, df, [col_name], cache_key)
                computed_match_level.add(col_name)

        # Track which player_* columns we've computed (needed for mirroring)
        computed_player_cols: set[str] = set()
        # Track which opp_* columns we need to mirror
        opp_cols_to_mirror: list[tuple[str, str]] = []  # (player_col, opp_col)

        # Phase 1: compute all base player_* features and identify opp_* needs
        for spec in base_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)

            # Build column name from full_name and params
            col_name = build_column_name(full_name, params)

            if prefix == "player":
                player_col = col_name
                # Compute if not already done
                if player_col not in computed_player_cols:
                    # Build the internal spec for caching (uses base name)
                    cache_spec = f"player_{base_name}"
                    if params:
                        param_str = ",".join(f"{k}={v}" for k, v in params.items())
                        cache_spec = f"player_{base_name}({param_str})"

                    if self._is_cached(cache_spec, cache_key):
                        cached = self._load_cached_feature(cache_spec)
                        df = df.join(
                            cached,
                            on=["match_uid", "player_id"],
                            how="left",
                        )
                    else:
                        expr = feature_def.func(**params)
                        df = df.with_columns(expr.alias(player_col))
                        self._cache_feature(
                            cache_spec, df, [player_col], cache_key
                        )
                    computed_player_cols.add(player_col)

            elif prefix == "opp":
                # We need the player version first, then mirror
                player_col = f"player_{base_name}"
                if params:
                    player_col = build_column_name(f"player_{base_name}", params)
                opp_col = col_name
                opp_cols_to_mirror.append((player_col, opp_col))

                # Compute player version if not already done
                if player_col not in computed_player_cols:
                    cache_spec = f"player_{base_name}"
                    if params:
                        param_str = ",".join(f"{k}={v}" for k, v in params.items())
                        cache_spec = f"player_{base_name}({param_str})"

                    if self._is_cached(cache_spec, cache_key):
                        cached = self._load_cached_feature(cache_spec)
                        df = df.join(
                            cached,
                            on=["match_uid", "player_id"],
                            how="left",
                        )
                    else:
                        expr = feature_def.func(**params)
                        df = df.with_columns(expr.alias(player_col))
                        self._cache_feature(
                            cache_spec, df, [player_col], cache_key
                        )
                    computed_player_cols.add(player_col)

        # Phase 2: mirror to create opp_* columns (BEFORE derived features)
        if opp_cols_to_mirror:
            player_cols_for_mirror = list({p for p, _ in opp_cols_to_mirror})
            df = self._mirror_features(df, player_cols_for_mirror)

        # Phase 3: compute derived features (those with depends_on)
        # Identify which derived features need opp_* mirroring so we can
        # defer player_ features that depend on those opp_* columns.
        opp_derived_bases: set[str] = set()
        for spec in derived_specs:
            prefix, base_name, _full, _params = parse_feature_spec(spec)
            if prefix == "opp":
                opp_derived_bases.add(base_name)

        # First pass: compute player_ derived features that DON'T need
        # opp_* derived columns (which haven't been mirrored yet).
        deferred_specs: list[str] = []
        derived_opp_to_mirror: list[str] = []

        for spec in derived_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)
            col_name = build_column_name(full_name, params)

            if prefix == "opp" and feature_def.mirror:
                player_col = f"player_{base_name}"
                if params:
                    player_col = build_column_name(f"player_{base_name}", params)
                derived_opp_to_mirror.append(player_col)
                continue

            if prefix == "player" and col_name not in computed_player_cols:
                # Defer features that directly reference opp_ columns in their
                # expression (mirror=False + depends on derived opp_ feature).
                # Mirrored features only use player_ columns, so they can
                # compute now even if their dependencies have opp_ variants.
                needs_opp_derived = (
                    not feature_def.mirror
                    and any(
                        dep in opp_derived_bases for dep in feature_def.depends_on
                    )
                )
                if needs_opp_derived:
                    deferred_specs.append(spec)
                    continue

                df = self._compute_player_derived(
                    df, base_name, col_name, params, feature_def,
                    cache_key, computed_player_cols,
                )

        # Phase 4: mirror derived features that need opp_* columns
        if derived_opp_to_mirror:
            df = self._mirror_features(df, derived_opp_to_mirror)

        # Phase 5: compute deferred derived features (those that needed
        # opp_* derived columns, e.g., matchup_aggressor_vs_counterpuncher)
        for spec in deferred_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)
            col_name = build_column_name(full_name, params)

            if col_name not in computed_player_cols:
                df = self._compute_player_derived(
                    df, base_name, col_name, params, feature_def,
                    cache_key, computed_player_cols,
                )

        # Phase 6: compute match-level derived features (no prefix, has depends_on).
        # Runs last so all player_*/opp_* columns are available.
        for spec in match_level_derived_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)

            if col_name not in computed_match_level:
                feature_def = self._registry.get(base_name)
                cache_spec = base_name
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"{base_name}({param_str})"

                if self._is_cached(cache_spec, cache_key):
                    cached = self._load_cached_feature(cache_spec)
                    df = df.join(
                        cached,
                        on=["match_uid", "player_id"],
                        how="left",
                    )
                else:
                    expr = feature_def.func(**params)
                    df = df.with_columns(expr.alias(col_name))
                    self._cache_feature(cache_spec, df, [col_name], cache_key)
                computed_match_level.add(col_name)

        return df

    def _compute_player_derived(
        self,
        df: pl.DataFrame,
        base_name: str,
        col_name: str,
        params: dict,
        feature_def: "FeatureDef",
        cache_key: str,
        computed_player_cols: set[str],
    ) -> pl.DataFrame:
        """Compute a single player-prefixed derived feature."""
        cache_spec = f"player_{base_name}"
        if params:
            param_str = ",".join(f"{k}={v}" for k, v in params.items())
            cache_spec = f"player_{base_name}({param_str})"

        if self._is_cached(cache_spec, cache_key):
            cached = self._load_cached_feature(cache_spec)
            df = df.join(
                cached,
                on=["match_uid", "player_id"],
                how="left",
            )
        else:
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)
        computed_player_cols.add(col_name)
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

    def coverage_report(self, df: pl.DataFrame) -> dict[str, dict[str, Any]]:
        """Generate a coverage report for computed features.

        Analyzes the computed feature columns (player_* and opp_*) and reports
        null statistics for each.

        Args:
            df: DataFrame with computed feature columns.

        Returns:
            Dictionary mapping column names to statistics:
            - null_count: Number of null values
            - null_pct: Percentage of null values (0-100)
            - total_rows: Total number of rows
        """
        report: dict[str, dict[str, Any]] = {}
        total_rows = len(df)

        # Find all feature columns (player_* and opp_*)
        feature_cols = [
            col
            for col in df.columns
            if col.startswith("player_") or col.startswith("opp_")
        ]

        for col in feature_cols:
            null_count = df[col].null_count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0

            report[col] = {
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
                "total_rows": total_rows,
            }

        return report
