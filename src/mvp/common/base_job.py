"""Base class for pipeline jobs providing file I/O and path management."""

import hashlib
import json
import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

BUCKETS = ("raw", "stage", "analytics")


class BaseJob:
    """Base class providing file I/O and path management for pipeline jobs."""

    def __init__(self, domain: str, data_root: Path | None = None):
        if data_root is None:
            data_root = Path(__file__).resolve().parents[3] / "data"
        self.domain = domain
        self.data_root = data_root

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
    ) -> Path:
        """Build absolute path within the data directory."""
        if bucket not in BUCKETS:
            raise ValueError(
                f"Invalid bucket '{bucket}'. Must be one of: {', '.join(BUCKETS)}"
            )
        path = self.data_root / bucket / self.domain / relative_path
        if filename is not None:
            path = path / filename
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

    def save_parquet(self, df: pl.DataFrame, path: Path) -> Path | None:
        """Save DataFrame to parquet with schema hash metadata.

        Returns None if the DataFrame is empty.
        """
        if df.is_empty():
            logger.warning("Skipping empty parquet write: %s", path)
            return None
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        schema_str = json.dumps(
            [(col, str(dtype)) for col, dtype in df.schema.items()]
        )
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:16]
        try:
            df.write_parquet(
                tmp_path,
                metadata={"schema_hash": schema_hash},
            )
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        logger.info("Saved parquet to %s", self._display_path(path))
        return path

    def list_files(self, directory: Path, pattern: str = "*") -> list[Path]:
        """List files matching a glob pattern, sorted by name."""
        if not directory.is_dir():
            return []
        return sorted(directory.glob(pattern))
