"""Configuration schema for lines forward-selection discovery.

Shape mirrors `ProjectionDiscoveryConfig` (data / discovery / model / validation).
The `discovery.target` field is the discriminator that distinguishes lines
configs from other discovery types in the experiment dispatcher.
"""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel

from mvp.model.config import DataConfig, ValidationConfig


Target = Literal["total", "spread", "player_games"]


class LinesFeaturesConfig(BaseModel):
    """Feature configuration for lines discovery."""

    include: list[str] = []          # candidate pool (empty → full match-level pool)
    exclude: list[str] = []          # blocklist applied after pool resolution
    base: list[str] = []             # always kept; FS starts from these
    window_sizes: list[int] | None = None
    max: int | None = None           # cap on total selected (base + FS additions)


class LinesDiscoveryOptions(BaseModel):
    """Discovery-specific options for the lines proxy."""

    target: Target
    metric: Literal["log_loss", "cal_max", "cal_sum", "brier"] = "log_loss"
    selection_method: Literal["forward"] = "forward"
    min_delta: float = 0.0001
    features: LinesFeaturesConfig = LinesFeaturesConfig()

    # Per-target line grids; only the one matching `target` is used.
    total_lines: list[float] = [18.5, 19.5, 20.5, 21.5, 22.5, 23.5, 24.5, 25.5]
    spread_lines: list[float] = [-5.5, -4.5, -3.5, -2.5, -1.5, 1.5, 2.5, 3.5, 4.5, 5.5]
    player_games_lines: list[float] = [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5]

    @property
    def active_lines(self) -> list[float]:
        if self.target == "total":
            return list(self.total_lines)
        if self.target == "spread":
            return list(self.spread_lines)
        if self.target == "player_games":
            return list(self.player_games_lines)
        raise ValueError(f"Unknown target: {self.target}")


class LinesModelConfig(BaseModel):
    """Per-line classifier config."""

    type: Literal["logistic", "xgboost"] = "xgboost"
    params: dict[str, Any] = {}


class LinesDiscoveryConfig(BaseModel):
    """Complete discovery configuration for the lines proxy."""

    description: str | None = None
    data: DataConfig
    discovery: LinesDiscoveryOptions
    model: LinesModelConfig = LinesModelConfig()
    validation: ValidationConfig = ValidationConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "LinesDiscoveryConfig":
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "LinesDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())
