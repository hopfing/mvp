"""Filesystem contract for the oddspapi source.

`B:/raw/oddspapi/` -> `B:/stage/oddspapi/`, mirroring every other source
(`raw/<source>/<dataset>/`, `stage/<source>/<market>.parquet`). The scrapers are
one-source-one-book by coincidence; oddspapi is one source carrying many books, so
`book` is a column rather than a directory.

Raw CAPTURE is out of scope (spec 2026-07-26-oddspapi-odds-ingest §1) — this module
only declares where capture is expected to write, so the stage layer reads from the
pipeline location rather than from a working directory under scripts/.
"""

from __future__ import annotations

from pathlib import Path

from mvp.common.base_job import get_data_root

SOURCE = "oddspapi"

# The books the capture pulls, in the fixed groups the endpoint's 3-slug maximum
# forces. Also the directory names under raw/oddspapi/historical/.
BOOK_GROUPS: list[list[str]] = [
    ["pinnacle", "draftkings", "betrivers"],
    ["bet365", "fanduel", "betmgm"],
]
ALL_BOOKS: list[str] = [b for g in BOOK_GROUPS for b in g]

# Book roles (spec §5). A book absent from both is an error, not a default: a new
# book has to be classified deliberately rather than silently becoming bettable.
REFERENCE_BOOKS: frozenset[str] = frozenset({"pinnacle"})
ENTRY_BOOKS: frozenset[str] = frozenset(ALL_BOOKS) - REFERENCE_BOOKS


def raw_root() -> Path:
    return get_data_root() / "raw" / SOURCE


def stage_root() -> Path:
    return get_data_root() / "stage" / SOURCE


def fixtures_dir() -> Path:
    return raw_root() / "fixtures"


def reference_dir() -> Path:
    return raw_root() / "reference"


def markets_reference() -> Path:
    return reference_dir() / "markets_tennis.json"


def historical_dirs() -> list[Path]:
    """One directory per book group; each holds a merged file per fixture."""
    return [raw_root() / "historical" / "+".join(g) for g in BOOK_GROUPS]


def aggregate_root() -> Path:
    """Cross-fixture output. `stage/` is 1:1 with raw; combining across fixtures is
    an aggregate, the same relation aggregate/atptour/matches.parquet has to the
    per-tournament stage files."""
    return get_data_root() / "aggregate" / SOURCE


def quotes_path() -> Path:
    return aggregate_root() / "quotes.parquet"


def crosswalk_path() -> Path:
    """Cached fixture -> match_uid crosswalk, shared across markets.

    Resolution reads matches.parquet and rebuilds the player lookup, so redoing it
    per market would be pure waste.
    """
    return stage_root() / "_crosswalk.parquet"


def ticks_dir() -> Path:
    """Parsed ticks, one parquet per raw file, mirroring raw/historical/<group>/.

    Per-file staging rather than a manifest, following atptour
    (`atptour/pipeline.py:131-148`): the staged file's own existence and mtime are
    the skip signal, so there is no side-car table to drift from what is on disk.
    """
    return stage_root() / "ticks"


def ticks_path(group: str, raw_stem: str) -> Path:
    """Staged counterpart of one raw capture file.

    Keyed on the raw filename, not the fixture id inside it, so an unreadable file
    still gets a (zero-row) staged file and stops being re-parsed every run.
    """
    return ticks_dir() / group / f"{raw_stem}.parquet"


def ticks_schema_marker() -> Path:
    """Schema hash the staged ticks tree was written under.

    Every staged file also carries the hash in its own parquet metadata (see
    BaseJob.save_parquet), which is what `is_schema_current` reads. This marker
    exists because reading it per file costs 233s across 16.5k files, while the
    hash is a property of the parser, not of any one file: one read here, and a
    mismatch re-stages the whole tree. Over-invalidates, never under-invalidates.
    Sentinel-in-stage-dir follows `stage/atptour/.activity_refresh_marker`.
    """
    return ticks_dir() / ".schema_hash"


def ensure_dirs() -> None:
    """Create the raw and stage trees. Safe to call repeatedly.

    The raw subdirectories are created even though capture is out of scope, so the
    expected layout exists and a capture job has somewhere to land.
    """
    for d in (fixtures_dir(), reference_dir(), *historical_dirs(),
              stage_root(), aggregate_root()):
        d.mkdir(parents=True, exist_ok=True)
    for g in historical_dirs():
        (ticks_dir() / g.name).mkdir(parents=True, exist_ok=True)


def role_of(book: str) -> str:
    if book in REFERENCE_BOOKS:
        return "reference"
    if book in ENTRY_BOOKS:
        return "entry"
    raise ValueError(
        f"book {book!r} has no role. Classify it in paths.REFERENCE_BOOKS or "
        f"ENTRY_BOOKS — defaulting an unknown book to entry would let the backtest "
        f"price bets at a book that cannot be reached."
    )
