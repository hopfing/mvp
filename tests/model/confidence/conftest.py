"""Shared fixtures for confidence validation tests."""

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest


@pytest.fixture
def rng():
    return np.random.default_rng(42)


@pytest.fixture
def make_oof_df():
    """Factory for synthetic OOF DataFrames."""

    def _make(
        n: int = 1000,
        date_start: date = date(2022, 1, 1),
        date_end: date = date(2024, 12, 31),
        circuits: list[str] | None = None,
        surfaces: list[str] | None = None,
        rounds: list[str] | None = None,
        cal_bias: float = 0.0,
        seed: int = 42,
    ) -> pl.DataFrame:
        rng = np.random.default_rng(seed)
        circuits = circuits or ["chal", "tour"]
        surfaces = surfaces or ["hard", "clay", "grass"]
        rounds = rounds or ["Q1", "R32", "R16", "QF", "SF", "F"]

        days_range = (date_end - date_start).days
        day_offsets = np.sort(rng.integers(0, days_range, size=n))
        dates = [date_start + timedelta(days=int(d)) for d in day_offsets]

        y_prob_raw = rng.uniform(0.3, 0.95, size=n)
        effective_prob = np.clip(y_prob_raw + cal_bias, 0.01, 0.99)
        y_true = (rng.random(n) < effective_prob).astype(int)

        base_elo = rng.normal(1500, 100, size=n)
        elo_gap = (y_prob_raw - 0.5) * 600
        player_elo = base_elo + elo_gap / 2
        opp_elo = base_elo - elo_gap / 2

        return pl.DataFrame({
            "match_uid": [f"m_{i:05d}" for i in range(n)],
            "effective_match_date": dates,
            "y_true": y_true,
            "y_prob": y_prob_raw,
            "circuit": [circuits[i % len(circuits)] for i in range(n)],
            "surface": [surfaces[i % len(surfaces)] for i in range(n)],
            "round": [rounds[i % len(rounds)] for i in range(n)],
            "player_elo": player_elo,
            "opp_elo": opp_elo,
            "player_elo_rd": rng.uniform(30, 120, size=n),
            "opp_elo_rd": rng.uniform(30, 120, size=n),
            "player_rank": rng.integers(1, 500, size=n),
            "opp_rank": rng.integers(1, 500, size=n),
            "player_birth_date": [date(1990 + i % 15, 1, 1) for i in range(n)],
            "opp_birth_date": [date(1990 + (i + 3) % 15, 1, 1) for i in range(n)],
        }).with_columns(
            pl.col("effective_match_date").cast(pl.Date),
            pl.col("player_birth_date").cast(pl.Date),
            pl.col("opp_birth_date").cast(pl.Date),
        )

    return _make
