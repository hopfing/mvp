"""Checkpoint state for resumable forward feature selection."""

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class SelectionCheckpoint:
    """Serializable snapshot of forward selection progress."""

    run_name: str
    started_at: datetime
    updated_at: datetime
    completed_rounds: list[dict]           # [{"feature": str, "metric": float}, ...]
    current_round: int                     # 1-indexed round in progress
    total_candidates: int                  # total candidates in current round
    current_round_scores: dict[str, float] # {candidate: score} evaluated so far
    best_metric: float                     # best metric after last completed round
    direction: str                         # "minimize" or "maximize"
    max_features: int                      # max rounds


def save_checkpoint(path: Path, cp: SelectionCheckpoint) -> None:
    """Atomically write checkpoint to disk."""
    data = asdict(cp)
    data["started_at"] = cp.started_at.isoformat()
    data["updated_at"] = cp.updated_at.isoformat()
    # JSON has no native inf/-inf representation
    if data["best_metric"] == float("inf"):
        data["best_metric"] = "Infinity"
    elif data["best_metric"] == float("-inf"):
        data["best_metric"] = "-Infinity"

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2))
    os.replace(str(tmp_path), str(path))


def load_checkpoint(path: Path) -> SelectionCheckpoint | None:
    """Load checkpoint from disk, or None if the file does not exist."""
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    data["started_at"] = datetime.fromisoformat(data["started_at"])
    data["updated_at"] = datetime.fromisoformat(data["updated_at"])
    if data["best_metric"] == "Infinity":
        data["best_metric"] = float("inf")
    elif data["best_metric"] == "-Infinity":
        data["best_metric"] = float("-inf")
    return SelectionCheckpoint(**data)


def format_checkpoint_info(cp: SelectionCheckpoint) -> str:
    """Format checkpoint metadata for the CLI gate message."""
    n_scored = len(cp.current_round_scores)
    updated = cp.updated_at.strftime("%Y-%m-%d %H:%M")
    return (
        f"Checkpoint found for '{cp.run_name}' "
        f"(last updated: {updated}, "
        f"round {cp.current_round}/{cp.max_features}, "
        f"{n_scored}/{cp.total_candidates} candidates)"
    )
