"""Corpus validation: run rankings transformer against full raw data.

Reads real HTML files from data/raw/atptour/rankings/ and validates the
transformer produces expected volumes. Marked slow — run explicitly.
"""

import logging
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.transformers.rankings import RankingsTransformer

logger = logging.getLogger(__name__)


def _find_data_root() -> Path:
    """Find the data root, handling git worktrees."""
    repo_root = Path(__file__).resolve().parent
    while repo_root != repo_root.parent:
        if (repo_root / "pyproject.toml").exists():
            break
        repo_root = repo_root.parent

    data_dir = repo_root / "data"
    if data_dir.is_dir():
        return data_dir

    git_path = repo_root / ".git"
    if git_path.is_file():
        gitdir_line = git_path.read_text().strip()
        if gitdir_line.startswith("gitdir:"):
            gitdir = Path(gitdir_line.split(":", 1)[1].strip())
            main_repo = gitdir.resolve().parents[2]
            main_data = main_repo / "data"
            if main_data.is_dir():
                return main_data

    return data_dir


DATA_ROOT = _find_data_root()


@pytest.mark.slow
class TestCorpusValidation:
    def test_process_full_corpus(self):
        """Process all 933 rankings files and validate volume."""
        raw_dir = DATA_ROOT / "raw" / "atptour" / "rankings"
        html_files = sorted(raw_dir.glob("rankings_singles_*.html"))
        if not html_files:
            pytest.skip("No raw rankings data found — run from project root")

        xf = RankingsTransformer(data_root=DATA_ROOT)
        paths = xf.run()

        logger.info("Processed %d HTML files", len(html_files))
        logger.info("Wrote %d parquet files", len(paths))

        total_records = sum(len(pl.read_parquet(p)) for p in paths)
        logger.info("Total records: %d", total_records)

        failures = len(html_files) - len(paths)
        assert failures == 0, f"{failures} files failed to produce output"

        # 933 files × ~2,200 players ≈ 2M records
        assert total_records > 1_500_000, (
            f"Expected >1.5M records, got {total_records}"
        )

    def test_consolidate_full_corpus(self):
        """Consolidate all per-date parquets into one file."""
        raw_dir = DATA_ROOT / "raw" / "atptour" / "rankings"
        html_files = sorted(raw_dir.glob("rankings_singles_*.html"))
        if not html_files:
            pytest.skip("No raw rankings data found — run from project root")

        xf = RankingsTransformer(data_root=DATA_ROOT)
        per_date_paths = xf.run()
        result = xf.consolidate()

        assert result is not None
        df = pl.read_parquet(result)

        per_date_total = sum(len(pl.read_parquet(p)) for p in per_date_paths)
        assert len(df) == per_date_total, (
            f"Consolidated has {len(df)} rows but per-date total is "
            f"{per_date_total}"
        )

        logger.info(
            "Consolidated: %d rows from %d files",
            len(df),
            len(per_date_paths),
        )
