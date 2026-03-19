"""Base class for pipeline jobs providing file I/O and path management."""

import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

BUCKETS = ("raw", "stage", "aggregate", "analytics")


def get_data_root() -> Path:
    """Get the data root directory.

    Checks MVP_DATA_ROOT env var first, falls back to <project>/data.
    """
    env = os.environ.get("MVP_DATA_ROOT")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[3] / "data"


class BaseJob:
    """Base class providing file I/O and path management for pipeline jobs."""

    def __init__(self, domain: str, data_root: Path | None = None):
        if data_root is None:
            data_root = get_data_root()
        self.domain = domain
        self.data_root = data_root
        self._run_dt = dt.datetime.now()
        self._run_date_str = self._run_dt.strftime("%Y%m%d")
        self._run_datetime_str = self._run_dt.strftime("%Y%m%d_%H%M%S")

    def _display_path(self, path: Path) -> Path | str:
        """Path relative to data_root for logging; falls back to full path."""
        try:
            return path.relative_to(self.data_root)
        except ValueError:
            return path

    def build_path(
        self,
        bucket: str,
        relative_path: str,
        filename: str | None = None,
        version: str | None = None,
    ) -> Path:
        """Build absolute path within the data directory."""
        if bucket not in BUCKETS:
            raise ValueError(
                f"Invalid bucket '{bucket}'. Must be one of: {', '.join(BUCKETS)}"
            )
        path = self.data_root / bucket / self.domain / relative_path
        if filename is not None:
            path = path / filename
        if version == "date":
            path = path.with_stem(f"{path.stem}_{self._run_date_str}")
        elif version == "datetime":
            path = path.with_stem(f"{path.stem}_{self._run_datetime_str}")
        elif version is not None:
            raise ValueError(
                f"Invalid version '{version}'. Must be 'date', 'datetime', or None."
            )
        return path

    def save_json(self, data: dict | list, path: Path) -> Path:
        """Save JSON data with atomic write."""
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        logger.info("Saved JSON to %s", self._display_path(path))
        return path

    def read_json(self, path: Path) -> dict | list:
        """Read JSON data from file."""
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("Read JSON from %s", self._display_path(path))
        return data

    def save_html(self, content: str, path: Path) -> Path:
        """Save HTML content with atomic write."""
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                f.write(content)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        logger.info("Saved HTML to %s", self._display_path(path))
        return path

    def read_html(self, path: Path) -> str:
        """Read HTML content from file."""
        with path.open("r", encoding="utf-8") as f:
            content = f.read()
        logger.info("Read HTML from %s", self._display_path(path))
        return content

    def save_parquet(
        self,
        df: pl.DataFrame,
        path: Path,
        *,
        schema_hash: str | None = None,
    ) -> Path | None:
        """Save DataFrame to parquet with schema hash metadata.

        Args:
            df: DataFrame to save.
            path: Target path.
            schema_hash: Pydantic SCHEMA_HASH for drift detection. If provided,
                stored in metadata for later comparison via is_schema_current().

        Returns None if the DataFrame is empty.
        """
        if df.is_empty():
            logger.warning("Skipping empty parquet write: %s", path)
            return None
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        # Compute polars schema hash for backwards compatibility
        schema_str = json.dumps(
            [(col, str(dtype)) for col, dtype in df.schema.items()]
        )
        polars_hash = hashlib.md5(schema_str.encode()).hexdigest()[:16]
        metadata = {"schema_hash": polars_hash}
        if schema_hash is not None:
            metadata["pydantic_schema_hash"] = schema_hash
        try:
            df.write_parquet(tmp_path, metadata=metadata)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        logger.info("Saved parquet to %s", self._display_path(path))
        return path

    def is_schema_current(self, path: Path, expected_hash: str) -> bool:
        """Check if parquet file's schema hash matches expected.

        Returns False if:
        - File doesn't exist
        - File has no pydantic_schema_hash metadata
        - Hash doesn't match expected

        Use this in skip logic to detect schema drift and re-process stale files.
        """
        if not path.exists():
            return False
        try:
            import pyarrow.parquet as pq

            meta = pq.read_metadata(path)
            if meta.metadata is None:
                return False
            stored_hash = meta.metadata.get(b"pydantic_schema_hash")
            if stored_hash is None:
                return False
            return stored_hash.decode() == expected_hash
        except Exception:
            return False

    def list_files(self, directory: Path, pattern: str = "*") -> list[Path]:
        """List files matching a glob pattern, sorted by name."""
        if not directory.is_dir():
            return []
        return sorted(directory.glob(pattern))
