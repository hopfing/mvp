"""Corpus validation: run overview transformer against full raw data.

Reads real JSON files from data/raw/atptour/ and validates the transformer
produces expected volumes. Marked slow — run explicitly.
"""

import json
import logging
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.tournament import Tournament
from mvp.atptour.transformers.overview import OverviewTransformer
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
    """Discover all tournament-years with overview.json in the raw corpus."""
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
                overview_file = year_dir / "overview.json"
                if overview_file.exists():
                    with overview_file.open(encoding="utf-8") as f:
                        data = json.load(f)
                    tournaments.append(
                        Tournament(
                            tournament_id=tid_dir.name,
                            year=int(year_dir.name),
                            circuit=circuit,
                            location=data["Location"],
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
        failures = []

        for t in tournaments:
            transformer = OverviewTransformer(tournament=t, data_root=DATA_ROOT)
            try:
                paths = transformer.run()
                for p in paths:
                    df = pl.read_parquet(p)
                    total_records += len(df)
            except Exception as e:
                failures.append((t.logging_id, str(e)))

        logger.info("Processed %d tournaments", len(tournaments))
        logger.info("Total records: %d", total_records)
        logger.info("Failures: %d", len(failures))
        for name, err in failures[:20]:
            logger.error("  %s: %s", name, err)

        assert len(failures) == 0, (
            f"{len(failures)} tournaments failed:\n"
            + "\n".join(f"  {n}: {e}" for n, e in failures[:20])
        )
        # v2 had 4,550 tournament-years with overview data
        assert total_records > 4_400, (
            f"Expected >4,400 records, got {total_records}"
        )
