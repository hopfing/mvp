"""Feature Engine for computing features from matches data."""


import ctypes
import ctypes.wintypes
import hashlib
import inspect
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import polars as pl

import mvp.model.features  # noqa: F401 - triggers feature registration
from mvp.model.registry import get_registry

logger = logging.getLogger(__name__)

_MEMORY_LIMIT_PCT = int(os.environ.get("MVP_MEMORY_LIMIT_PCT", "75"))


class MemoryLimitExceeded(RuntimeError):
    """Raised when process memory usage exceeds the configured threshold."""


def check_memory(context: str = "") -> None:
    """Abort if system memory usage exceeds the configured threshold.

    Uses Windows GlobalMemoryStatusEx to check physical memory usage.
    No-op on non-Windows platforms.
    """
    if _MEMORY_LIMIT_PCT <= 0 or _MEMORY_LIMIT_PCT >= 100:
        return
    if not hasattr(ctypes, "windll"):
        return

    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.wintypes.DWORD),
            ("dwMemoryLoad", ctypes.wintypes.DWORD),
            ("ullTotalPhys", ctypes.c_uint64),
            ("ullAvailPhys", ctypes.c_uint64),
            ("ullTotalPageFile", ctypes.c_uint64),
            ("ullAvailPageFile", ctypes.c_uint64),
            ("ullTotalVirtual", ctypes.c_uint64),
            ("ullAvailVirtual", ctypes.c_uint64),
            ("ullAvailExtendedVirtual", ctypes.c_uint64),
        ]

    stat = MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(stat)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
        return

    used_pct = stat.dwMemoryLoad
    if used_pct >= _MEMORY_LIMIT_PCT:
        used_gb = (stat.ullTotalPhys - stat.ullAvailPhys) / (1024 ** 3)
        total_gb = stat.ullTotalPhys / (1024 ** 3)
        msg = (
            f"Memory usage {used_pct}% ({used_gb:.1f}/{total_gb:.1f} GB) "
            f"exceeds limit of {_MEMORY_LIMIT_PCT}%"
        )
        if context:
            msg = f"[{context}] {msg}"
        raise MemoryLimitExceeded(msg)


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
        from old code are not served.  Falls back to bytecode if source
        files changed on disk after import (inspect.getsource fails).
        """
        sources = []
        for name in sorted(self._registry.list_features()):
            feat = self._registry.get(name)
            try:
                sig = inspect.getsource(feat.func)
            except OSError:
                sig = feat.func.__code__.co_code.hex()
            sources.append(f"{name}:{sig}")
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

    def _batch_join_cached(
        self, df: pl.DataFrame, cache_specs: list[str]
    ) -> pl.DataFrame:
        """Load multiple cached features and join into df in a single pass.

        Instead of N sequential joins on the wide matches DataFrame,
        this merges all cached parquets into one narrow frame first,
        then does a single join into df.
        """
        if not cache_specs:
            return df

        join_on = ["match_uid", "player_id"]
        frames = [self._load_cached_feature(spec) for spec in cache_specs]

        merged = frames[0]
        for frame in frames[1:]:
            merged = merged.join(frame, on=join_on, how="left")

        return df.join(merged, on=join_on, how="left")

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

    def _resolve_source_columns(
        self, feature_specs: list[str], extra_columns: list[str] | None = None,
    ) -> list[str]:
        """Determine which parquet columns are needed for the given features.

        Introspects base (non-derived) feature expressions via
        ``expr.meta.root_names()`` to find referenced source columns.
        Derived features reference computed columns, so they are skipped.

        Args:
            feature_specs: Resolved (dependency-expanded) feature specs.
            extra_columns: Additional columns the caller needs (e.g., for
                filtering, target resolution, diagnostics).

        Returns:
            Deduplicated list of parquet column names to load.
        """
        # Structural columns the engine always needs
        needed: set[str] = {
            "match_uid", "player_id", "opp_id", "effective_match_date",
        }

        if extra_columns:
            needed.update(extra_columns)

        # Introspect base (non-derived) feature expressions
        seen_bases: set[str] = set()
        for spec in feature_specs:
            _prefix, base_name, _full_name, params = parse_feature_spec(spec)
            if base_name in seen_bases:
                continue
            seen_bases.add(base_name)

            feature_def = self._registry.get(base_name)
            if feature_def.depends_on:
                continue  # derived — references computed columns, not source

            try:
                expr = feature_def.func(**params)
                root_names = expr.meta.root_names()
                needed.update(root_names)
            except Exception:
                logger.debug(
                    "Could not introspect columns for %s, will load all",
                    base_name,
                )
                return []  # fall back to loading everything

        # Validate against actual parquet schema
        available = set(
            pl.scan_parquet(self.matches_path).collect_schema().names()
        )
        pruned = sorted(needed & available)
        missing = needed - available
        if missing:
            logger.debug("Requested columns not in parquet (computed?): %s", missing)

        return pruned

    def ensure_cached(
        self,
        feature_specs: list[str],
        extra_columns: list[str] | None = None,
        batch_size: int = 150,
    ) -> str:
        """Compute all features and cache them without holding them all in memory.

        Processes base features in batches: compute batch, cache to disk, drop
        computed columns from the DataFrame before the next batch. Derived
        features load only their dependencies from cache.

        Args:
            feature_specs: Feature specs (will be dependency-resolved).
            extra_columns: Additional parquet columns to load.
            batch_size: Number of base features to compute per batch.

        Returns:
            The cache_key (callers need it to load from cache).
        """
        t0 = time.perf_counter()

        # Resolve dependencies
        feature_specs = self._resolve_dependencies(feature_specs)

        # Categorize
        match_level_specs: list[str] = []
        match_level_derived_specs: list[str] = []
        base_specs: list[str] = []
        derived_specs: list[str] = []
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

        logger.info(
            "ensure_cached: %d features (%d base, %d derived, %d match-level)",
            len(feature_specs), len(base_specs), len(derived_specs),
            len(match_level_specs) + len(match_level_derived_specs),
        )

        # Load source data (pruned columns)
        columns_to_load = self._resolve_source_columns(
            feature_specs, extra_columns,
        )
        if columns_to_load:
            df = pl.read_parquet(self.matches_path, columns=columns_to_load)
        else:
            df = pl.read_parquet(self.matches_path)
        logger.info("Loaded matches: %d rows x %d columns", df.height, df.width)
        check_memory("ensure_cached: after parquet load")

        # Cache validity
        cache_key = self._compute_cache_key()
        if self._manifest.get("cache_key") != cache_key:
            logger.info("Cache invalidated — recomputing all features")
            self._invalidate_cache()

        # Phase 0: match-level features (small, do all at once)
        for spec in match_level_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            cache_spec = base_name
            if params:
                param_str = ",".join(f"{k}={v}" for k, v in params.items())
                cache_spec = f"{base_name}({param_str})"
            if not self._is_cached(cache_spec, cache_key):
                feature_def = self._registry.get(base_name)
                expr = feature_def.func(**params)
                df = df.with_columns(expr.alias(col_name))
                self._cache_feature(cache_spec, df, [col_name], cache_key)
                df = df.drop(col_name)

        if match_level_specs:
            logger.info("Phase 0: %d match-level cached", len(match_level_specs))

        # Phase 1: base player_* features in batches
        # Deduplicate to player_ versions
        uncached_base: list[tuple[str, str, str, dict]] = []
        seen_player_cols: set[str] = set()
        for spec in base_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            player_col = build_column_name(
                f"player_{base_name}" if prefix == "opp" else full_name, params,
            )
            if player_col in seen_player_cols:
                continue
            seen_player_cols.add(player_col)
            cache_spec = f"player_{base_name}"
            if params:
                param_str = ",".join(f"{k}={v}" for k, v in params.items())
                cache_spec = f"player_{base_name}({param_str})"
            if not self._is_cached(cache_spec, cache_key):
                uncached_base.append((base_name, cache_spec, player_col, params))

        # Process uncached base features in batches
        for i in range(0, len(uncached_base), batch_size):
            batch = uncached_base[i : i + batch_size]
            for base_name, cache_spec, player_col, params in batch:
                feature_def = self._registry.get(base_name)
                expr = feature_def.func(**params)
                df = df.with_columns(expr.alias(player_col))
                self._cache_feature(cache_spec, df, [player_col], cache_key)
            # Drop computed columns before next batch
            cols_to_drop = [col for _, _, col, _ in batch if col in df.columns]
            if cols_to_drop:
                df = df.drop(cols_to_drop)
            check_memory(f"ensure_cached: after base batch {i // batch_size + 1}")

        logger.info(
            "Phase 1: %d base features (%d computed, %d already cached)",
            len(seen_player_cols), len(uncached_base),
            len(seen_player_cols) - len(uncached_base),
        )

        # Phase 3: derived features — load dependencies from cache, compute, cache, drop
        # First, handle opp mirroring: for opp derived features with mirror=True,
        # we need the player_ version computed and mirrored. For ensure_cached we
        # just need to compute the player_ version (mirroring happens at load time).
        for spec in derived_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            feature_def = self._registry.get(base_name)
            col_name = build_column_name(full_name, params)

            # opp_ derived with mirror: player_ version is what gets cached
            if prefix == "opp" and feature_def.mirror:
                prefix = "player"
                full_name = f"player_{base_name}"
                col_name = build_column_name(full_name, params)

            if prefix == "opp":
                continue  # opp_ non-mirror derived: handled via mirror at load time

            cache_spec = f"player_{base_name}"
            if params:
                param_str = ",".join(f"{k}={v}" for k, v in params.items())
                cache_spec = f"player_{base_name}({param_str})"
            if self._is_cached(cache_spec, cache_key):
                continue

            # Load dependencies from cache onto df
            dep_cache_specs = []
            for dep_name in feature_def.depends_on:
                for dep_prefix in ["player", "opp"]:
                    dep_spec = f"{dep_prefix}_{dep_name}"
                    if params:
                        param_str = ",".join(f"{k}={v}" for k, v in params.items())
                        dep_spec = f"{dep_prefix}_{dep_name}({param_str})"
                    # Load player_ version from cache
                    player_dep_spec = f"player_{dep_name}"
                    if params:
                        player_dep_spec = f"player_{dep_name}({param_str})"
                    if player_dep_spec not in dep_cache_specs:
                        dep_cache_specs.append(player_dep_spec)

            # Join deps, compute, cache, drop
            dep_cols_before = set(df.columns)
            df = self._batch_join_cached(df, dep_cache_specs)
            # Mirror player_ deps to opp_ if needed
            player_dep_cols = [
                c for c in df.columns
                if c not in dep_cols_before and c.startswith("player_")
            ]
            if player_dep_cols:
                df = self._mirror_features(df, player_dep_cols)

            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)

            # Drop everything we added
            added_cols = [c for c in df.columns if c not in dep_cols_before]
            if added_cols:
                df = df.drop(added_cols)

        if derived_specs:
            logger.info("Phase 3: derived features cached")
            check_memory("ensure_cached: after derived")

        # Phase 6: match-level derived features
        for spec in match_level_derived_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            cache_spec = base_name
            if params:
                param_str = ",".join(f"{k}={v}" for k, v in params.items())
                cache_spec = f"{base_name}({param_str})"
            if self._is_cached(cache_spec, cache_key):
                continue

            feature_def = self._registry.get(base_name)
            # Load dependencies
            dep_cols_before = set(df.columns)
            dep_cache_specs = []
            for dep_name in feature_def.depends_on:
                for dep_prefix in ["player", "opp"]:
                    player_dep_spec = f"player_{dep_name}"
                    if params:
                        param_str = ",".join(f"{k}={v}" for k, v in params.items())
                        player_dep_spec = f"player_{dep_name}({param_str})"
                    if player_dep_spec not in dep_cache_specs:
                        dep_cache_specs.append(player_dep_spec)
            df = self._batch_join_cached(df, dep_cache_specs)
            player_dep_cols = [
                c for c in df.columns
                if c not in dep_cols_before and c.startswith("player_")
            ]
            if player_dep_cols:
                df = self._mirror_features(df, player_dep_cols)

            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)
            added_cols = [c for c in df.columns if c not in dep_cols_before]
            if added_cols:
                df = df.drop(added_cols)

        elapsed = time.perf_counter() - t0
        logger.info("ensure_cached complete in %.1fs", elapsed)
        return cache_key

    def load_features_numpy(
        self,
        feature_specs: list[str],
        base_df: pl.DataFrame,
        cache_key: str,
    ) -> pl.DataFrame:
        """Load computed features from cache onto a (filtered) base DataFrame.

        Loads features one at a time from cache, joins to base_df, avoiding
        ever holding all features in memory as one wide DataFrame.

        Args:
            feature_specs: Feature specs to load (must already be cached).
            base_df: Filtered DataFrame with at least match_uid and player_id.
            cache_key: Cache key from ensure_cached().

        Returns:
            base_df with feature columns joined on.
        """
        join_on = ["match_uid", "player_id"]

        # Group specs by their cache spec to avoid duplicate loads
        specs_to_load: list[tuple[str, str]] = []  # (cache_spec, col_name)
        seen: set[str] = set()

        for spec in feature_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            feature_def = self._registry.get(base_name)

            if prefix == "opp" and feature_def.mirror:
                # Need the player_ version, will mirror later
                player_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    player_spec = f"player_{base_name}({param_str})"
                player_col = build_column_name(f"player_{base_name}", params)
                if player_col not in seen:
                    specs_to_load.append((player_spec, player_col))
                    seen.add(player_col)
            elif prefix == "opp":
                # opp_ non-mirror: load player_ version, will mirror
                player_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    player_spec = f"player_{base_name}({param_str})"
                player_col = build_column_name(f"player_{base_name}", params)
                if player_col not in seen:
                    specs_to_load.append((player_spec, player_col))
                    seen.add(player_col)
            elif prefix == "player":
                cache_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"player_{base_name}({param_str})"
                if col_name not in seen:
                    specs_to_load.append((cache_spec, col_name))
                    seen.add(col_name)
            else:
                # match-level
                cache_spec = base_name
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"{base_name}({param_str})"
                if col_name not in seen:
                    specs_to_load.append((cache_spec, col_name))
                    seen.add(col_name)

        # Load features from cache in batches to limit join overhead
        batch_size = 50
        for i in range(0, len(specs_to_load), batch_size):
            batch = specs_to_load[i : i + batch_size]
            frames = []
            for cache_spec, _col_name in batch:
                cached = self._load_cached_feature(cache_spec)
                frames.append(cached)
            if frames:
                merged = frames[0]
                for frame in frames[1:]:
                    merged = merged.join(frame, on=join_on, how="left")
                base_df = base_df.join(merged, on=join_on, how="left")

        # Mirror player_ → opp_ for any opp specs
        player_cols_to_mirror: list[str] = []
        for spec in feature_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            if prefix == "opp":
                player_col = build_column_name(f"player_{base_name}", params)
                if player_col not in player_cols_to_mirror:
                    player_cols_to_mirror.append(player_col)

        if player_cols_to_mirror:
            base_df = self._mirror_features(base_df, player_cols_to_mirror)

        return base_df

    def compute(
        self,
        feature_specs: list[str],
        extra_columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Compute features for the given feature specifications.

        Args:
            feature_specs: List of feature specs like ["player_win_rate(days=30)"].
                Specs can start with "player_", "opp_", or have no prefix (match-level).
            extra_columns: Additional parquet columns to load (for filtering,
                target resolution, diagnostics, etc.).

        Returns:
            DataFrame with match data and computed feature columns.
        """
        t0 = time.perf_counter()

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

        logger.info(
            "Computing %d features (%d base, %d derived, %d match-level)",
            len(feature_specs), len(base_specs), len(derived_specs),
            len(match_level_specs) + len(match_level_derived_specs),
        )

        # Column pruning: only load parquet columns that features actually need
        columns_to_load = self._resolve_source_columns(
            feature_specs, extra_columns,
        )
        if columns_to_load:
            df = pl.read_parquet(self.matches_path, columns=columns_to_load)
            logger.info(
                "Loaded matches: %d rows x %d columns (pruned from parquet)",
                df.height, df.width,
            )
        else:
            df = pl.read_parquet(self.matches_path)
            logger.info("Loaded matches: %d rows x %d columns", df.height, df.width)
        check_memory("after parquet load")

        # Check cache validity — wipe everything if data or code changed
        cache_key = self._compute_cache_key()
        if self._manifest.get("cache_key") != cache_key:
            logger.info("Cache invalidated — recomputing all features")
            self._invalidate_cache()

        # Phase 0: compute match-level features (no prefix)
        computed_match_level: set[str] = set()
        cached_specs_p0: list[str] = []
        uncached_p0: list[tuple[str, str, str, dict]] = []

        for spec in match_level_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            if col_name in computed_match_level:
                continue
            cache_spec = base_name
            if params:
                param_str = ",".join(f"{k}={v}" for k, v in params.items())
                cache_spec = f"{base_name}({param_str})"
            if self._is_cached(cache_spec, cache_key):
                cached_specs_p0.append(cache_spec)
            else:
                uncached_p0.append((base_name, cache_spec, col_name, params))
            computed_match_level.add(col_name)

        df = self._batch_join_cached(df, cached_specs_p0)
        for base_name, cache_spec, col_name, params in uncached_p0:
            feature_def = self._registry.get(base_name)
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)

        if match_level_specs:
            logger.info(
                "Phase 0: %d match-level (%d cached, %d computed)",
                len(computed_match_level), len(cached_specs_p0), len(uncached_p0),
            )

        # Phase 1: compute all base player_* features and identify opp_* needs
        computed_player_cols: set[str] = set()
        opp_cols_to_mirror: list[tuple[str, str]] = []
        cached_specs_p1: list[str] = []
        uncached_p1: list[tuple[str, str, str, dict]] = []

        for spec in base_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)

            if prefix == "player":
                player_col = col_name
            else:  # opp
                player_col = build_column_name(f"player_{base_name}", params)
                opp_cols_to_mirror.append((player_col, col_name))

            if player_col not in computed_player_cols:
                cache_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"player_{base_name}({param_str})"
                if self._is_cached(cache_spec, cache_key):
                    cached_specs_p1.append(cache_spec)
                else:
                    uncached_p1.append((base_name, cache_spec, player_col, params))
                computed_player_cols.add(player_col)

        df = self._batch_join_cached(df, cached_specs_p1)
        for base_name, cache_spec, player_col, params in uncached_p1:
            feature_def = self._registry.get(base_name)
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(player_col))
            self._cache_feature(cache_spec, df, [player_col], cache_key)

        logger.info(
            "Phase 1: %d base features (%d cached, %d computed)",
            len(computed_player_cols), len(cached_specs_p1), len(uncached_p1),
        )
        check_memory("after phase 1")

        # Phase 2: mirror to create opp_* columns (BEFORE derived features)
        if opp_cols_to_mirror:
            player_cols_for_mirror = list({p for p, _ in opp_cols_to_mirror})
            df = self._mirror_features(df, player_cols_for_mirror)
            logger.info("Phase 2: mirrored %d columns", len(player_cols_for_mirror))

        # Phase 3: compute derived features (those with depends_on)
        opp_derived_bases: set[str] = set()
        for spec in derived_specs:
            prefix, base_name, _full, _params = parse_feature_spec(spec)
            if prefix == "opp":
                opp_derived_bases.add(base_name)

        deferred_specs: list[str] = []
        derived_opp_to_mirror: list[str] = []
        cached_specs_p3: list[str] = []
        uncached_p3: list[tuple[str, str, str, dict]] = []

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
                needs_opp_derived = (
                    not feature_def.mirror
                    and any(
                        dep in opp_derived_bases for dep in feature_def.depends_on
                    )
                )
                if needs_opp_derived:
                    deferred_specs.append(spec)
                    continue

                cache_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"player_{base_name}({param_str})"
                if self._is_cached(cache_spec, cache_key):
                    cached_specs_p3.append(cache_spec)
                    computed_player_cols.add(col_name)
                else:
                    uncached_p3.append((base_name, cache_spec, col_name, params))

        df = self._batch_join_cached(df, cached_specs_p3)
        for base_name, cache_spec, col_name, params in uncached_p3:
            feature_def = self._registry.get(base_name)
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)
            computed_player_cols.add(col_name)

        if derived_specs:
            logger.info(
                "Phase 3: %d derived (%d cached, %d computed, %d deferred)",
                len(cached_specs_p3) + len(uncached_p3), len(cached_specs_p3),
                len(uncached_p3), len(deferred_specs),
            )
            check_memory("after phase 3")

        # Phase 4: mirror derived features that need opp_* columns
        if derived_opp_to_mirror:
            df = self._mirror_features(df, derived_opp_to_mirror)
            logger.info("Phase 4: mirrored %d derived columns", len(derived_opp_to_mirror))

        # Phase 5: compute deferred derived features (those that needed
        # opp_* derived columns, e.g., matchup_aggressor_vs_counterpuncher)
        cached_specs_p5: list[str] = []
        uncached_p5: list[tuple[str, str, str, dict]] = []

        for spec in deferred_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            if col_name not in computed_player_cols:
                cache_spec = f"player_{base_name}"
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"player_{base_name}({param_str})"
                if self._is_cached(cache_spec, cache_key):
                    cached_specs_p5.append(cache_spec)
                    computed_player_cols.add(col_name)
                else:
                    uncached_p5.append((base_name, cache_spec, col_name, params))

        df = self._batch_join_cached(df, cached_specs_p5)
        for base_name, cache_spec, col_name, params in uncached_p5:
            feature_def = self._registry.get(base_name)
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)
            computed_player_cols.add(col_name)

        # Phase 6: compute match-level derived features (no prefix, has depends_on).
        # Runs last so all player_*/opp_* columns are available.
        cached_specs_p6: list[str] = []
        uncached_p6: list[tuple[str, str, str, dict]] = []

        for spec in match_level_derived_specs:
            _prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            if col_name not in computed_match_level:
                cache_spec = base_name
                if params:
                    param_str = ",".join(f"{k}={v}" for k, v in params.items())
                    cache_spec = f"{base_name}({param_str})"
                if self._is_cached(cache_spec, cache_key):
                    cached_specs_p6.append(cache_spec)
                else:
                    uncached_p6.append((base_name, cache_spec, col_name, params))
                computed_match_level.add(col_name)

        df = self._batch_join_cached(df, cached_specs_p6)
        for base_name, cache_spec, col_name, params in uncached_p6:
            feature_def = self._registry.get(base_name)
            expr = feature_def.func(**params)
            df = df.with_columns(expr.alias(col_name))
            self._cache_feature(cache_spec, df, [col_name], cache_key)

        elapsed = time.perf_counter() - t0
        logger.info("Feature computation complete in %.1fs", elapsed)

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
