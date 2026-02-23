"""Corpus validation: run match stats transformer against full raw data.

These tests read real JSON files from data/raw/atptour/ and validate
the transformer produces expected volumes. Marked slow — run explicitly.
"""

import logging
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.transformers.match_stats import MatchStatsTransformer
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit

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
RAW_TOURNAMENTS = DATA_ROOT / "raw" / "atptour" / "tournaments"


def _discover_tournaments() -> list[Tournament]:
    """Discover all tournament-years with match stats JSON in the raw corpus."""
    tournaments = []
    if not RAW_TOURNAMENTS.is_dir():
        return tournaments
    for circuit_dir in sorted(RAW_TOURNAMENTS.iterdir()):
        if not circuit_dir.is_dir():
            continue
        circuit_name = circuit_dir.name
        try:
            circuit = Circuit(circuit_name)
        except ValueError:
            continue
        for tid_dir in sorted(circuit_dir.iterdir()):
            if not tid_dir.is_dir():
                continue
            for year_dir in sorted(tid_dir.iterdir()):
                if not year_dir.is_dir():
                    continue
                match_stats_dir = year_dir / "match_stats"
                has_stats = match_stats_dir.is_dir() and any(match_stats_dir.glob("*.json"))
                if has_stats:
                    tournaments.append(
                        Tournament(
                            tournament_id=tid_dir.name,
                            year=int(year_dir.name),
                            circuit=circuit,
                            location=f"Unknown, {circuit_name}",
                        )
                    )
    return tournaments


@pytest.mark.slow
class TestCorpusValidation:
    def test_process_full_corpus(self):
        """Process all tournament-years and validate volume."""
        tournaments = _discover_tournaments()
        if not tournaments:
            pytest.skip("No raw data found — run from project root")

        total_records = 0
        total_singles = 0
        total_doubles = 0
        failures = []

        for t in tournaments:
            transformer = MatchStatsTransformer(tournament=t, data_root=DATA_ROOT)
            try:
                paths = transformer.run()
                for p in paths:
                    df = pl.read_parquet(p)
                    total_records += len(df)
                    total_singles += len(
                        df.filter(pl.col("draw_type") == "singles")
                    )
                    total_doubles += len(
                        df.filter(pl.col("draw_type") == "doubles")
                    )
            except Exception as e:
                failures.append((t.logging_id, str(e)))

        logger.info("Processed %d tournaments", len(tournaments))
        logger.info(
            "Total records: %d (singles: %d, doubles: %d)",
            total_records,
            total_singles,
            total_doubles,
        )
        logger.info("Failures: %d", len(failures))
        for name, err in failures[:20]:
            logger.error("  %s: %s", name, err)

        assert len(failures) == 0, (
            f"{len(failures)} tournaments failed:\n"
            + "\n".join(f"  {n}: {e}" for n, e in failures[:20])
        )
        # v2 had 216,963 rows (216,280 singles, 683 doubles)
        assert total_singles > 210_000, (
            f"Expected >210K singles, got {total_singles}"
        )
