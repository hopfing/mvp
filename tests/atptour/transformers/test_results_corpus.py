"""Corpus validation: run results transformer against full raw data.

These tests read real HTML files from data/raw/atptour/ and validate
the transformer produces expected volumes. Marked slow — run explicitly.
"""

import logging
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.transformers.results import ResultsTransformer
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)


def _find_data_root() -> Path:
    """Find the data root, handling git worktrees.

    Walks up from the test file to find the repo root (where data/ lives).
    In a worktree, data/ may only exist in the main repo, so we follow
    the .git file to find the main working tree if needed.
    """
    # Start from the test file and walk up to the repo root
    repo_root = Path(__file__).resolve().parent
    while repo_root != repo_root.parent:
        if (repo_root / "pyproject.toml").exists():
            break
        repo_root = repo_root.parent

    data_dir = repo_root / "data"
    if data_dir.is_dir():
        return data_dir

    # In a worktree, data/ may not exist — find the main repo
    git_path = repo_root / ".git"
    if git_path.is_file():
        # .git is a file in worktrees: "gitdir: /path/to/.git/worktrees/name"
        gitdir_line = git_path.read_text().strip()
        if gitdir_line.startswith("gitdir:"):
            gitdir = Path(gitdir_line.split(":", 1)[1].strip())
            # gitdir points to .git/worktrees/name — main repo is two levels up
            main_repo = gitdir.resolve().parents[2]
            main_data = main_repo / "data"
            if main_data.is_dir():
                return main_data

    return data_dir  # Fallback — will trigger skip in test


DATA_ROOT = _find_data_root()
RAW_TOURNAMENTS = DATA_ROOT / "raw" / "atptour" / "tournaments"


def _discover_tournaments() -> list[Tournament]:
    """Discover all tournament-years with results HTML in the raw corpus."""
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
                has_results = (
                    (year_dir / "results_singles.html").exists()
                    or (year_dir / "results_doubles.html").exists()
                )
                year = int(year_dir.name)
                if has_results:
                    tournaments.append(
                        Tournament(
                            tournament_id=tid_dir.name,
                            year=year,
                            circuit=circuit,
                            location=f"Unknown, {circuit_name}",
                        )
                    )
    return tournaments


@pytest.mark.slow
class TestCorpusValidation:
    def test_process_full_corpus(self, tmp_path):
        """Process all tournament-years and validate volume."""
        tournaments = _discover_tournaments()
        if not tournaments:
            pytest.skip("No raw data found — run from project root")

        total_records = 0
        total_singles = 0
        total_doubles = 0
        failures = []

        for t in tournaments:
            transformer = ResultsTransformer(tournament=t, data_root=DATA_ROOT)
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

        # Check failures first — more actionable than volume shortfall
        assert len(failures) == 0, (
            f"{len(failures)} tournaments failed:\n"
            + "\n".join(f"  {n}: {e}" for n, e in failures[:20])
        )
        # v2 had 243,499 singles rows with 869 duplicates = ~242,630 unique
        assert total_singles > 240_000, (
            f"Expected >240K singles, got {total_singles}"
        )
