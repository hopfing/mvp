# Serve/Return Elo Redesign — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace opponent-agnostic EMA serve/return Elo with true opponent-relative Elo using the standard logistic formula.

**Architecture:** Modify `update_serve_elo()` and `update_return_elo()` in ratings.py to use the Elo expected-score formula with normalized serve%. Update the serve/return block in compute.py to run two sub-games per match, updating all four ratings (both players' serve and return). Add one new constant.

**Tech Stack:** Python, Polars, pytest

**Design doc:** `docs/plans/2026-03-05-serve-return-elo-redesign.md`

---

### Task 1: Add new constant and normalize_serve_score helper

**Files:**
- Modify: `src/mvp/atptour/elo/constants.py`
- Modify: `src/mvp/atptour/elo/ratings.py`
- Test: `tests/atptour/elo/test_ratings.py`

**Step 1: Write the failing tests**

Add to `tests/atptour/elo/test_ratings.py`:

```python
from mvp.atptour.elo.ratings import normalize_serve_score


class TestNormalizeServeScore:
    """Test normalization of serve% to [0,1] score."""

    def test_at_baseline_returns_half(self):
        # Hard baseline is 0.62
        assert normalize_serve_score(0.62, "Hard") == 0.5

    def test_above_baseline_returns_above_half(self):
        score = normalize_serve_score(0.67, "Hard")
        assert score == 0.75

    def test_below_baseline_returns_below_half(self):
        score = normalize_serve_score(0.57, "Hard")
        assert score == 0.25

    def test_clamped_at_one(self):
        score = normalize_serve_score(0.85, "Hard")
        assert score == 1.0

    def test_clamped_at_zero(self):
        score = normalize_serve_score(0.40, "Hard")
        assert score == 0.0

    def test_clay_baseline(self):
        # Clay baseline is 0.60
        assert normalize_serve_score(0.60, "Clay") == 0.5

    def test_grass_baseline(self):
        # Grass baseline is 0.64
        assert normalize_serve_score(0.64, "Grass") == 0.5

    def test_unknown_surface_uses_default(self):
        # Unknown surface should use a sensible default (0.62)
        score = normalize_serve_score(0.62, "Carpet")
        assert score == 0.5
```

**Step 2: Run tests to verify they fail**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestNormalizeServeScore -v`
Expected: FAIL — `ImportError: cannot import name 'normalize_serve_score'`

**Step 3: Add constant and implement**

Add to `src/mvp/atptour/elo/constants.py`:

```python
# Score normalization for serve/return Elo
# Maps ±10pp from surface baseline to the full [0,1] range
SERVE_RETURN_DEVIATION_SCALE = 0.20
```

Add to `src/mvp/atptour/elo/ratings.py`:

```python
from mvp.atptour.elo.constants import (
    # ... existing imports ...
    SERVE_RETURN_DEVIATION_SCALE,
)


def normalize_serve_score(serve_pct: float, surface: str) -> float:
    """Normalize raw serve% to a [0,1] score centered at 0.5 for baseline.

    Uses surface-specific baselines. Clamps to [0, 1].
    """
    baseline = SERVE_BASELINE.get(surface, 0.62)
    score = (serve_pct - baseline) / SERVE_RETURN_DEVIATION_SCALE + 0.5
    return max(0.0, min(1.0, score))
```

**Step 4: Run tests to verify they pass**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestNormalizeServeScore -v`
Expected: PASS (all 8 tests)

**Step 5: Commit**

```
feat(elo): add normalize_serve_score helper for serve/return Elo redesign
```

---

### Task 2: Rewrite update_serve_elo with opponent-relative formula

**Files:**
- Modify: `src/mvp/atptour/elo/ratings.py`
- Test: `tests/atptour/elo/test_ratings.py`

**Step 1: Write the failing tests**

Replace the existing `TestUpdateServeElo` class in `tests/atptour/elo/test_ratings.py`:

```python
class TestUpdateServeElo:
    """Test opponent-relative serve Elo update."""

    def test_above_expected_increases_serve_elo(self):
        # Equal ratings, serve above baseline -> serve elo goes up
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.70,
            surface="Hard",
            k=12.8,
        )
        assert server_elo > 1500.0
        assert returner_elo < 1500.0  # zero-sum

    def test_below_expected_decreases_serve_elo(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.55,
            surface="Hard",
            k=12.8,
        )
        assert server_elo < 1500.0
        assert returner_elo > 1500.0  # zero-sum

    def test_at_baseline_equal_ratings_no_change(self):
        # At baseline with equal ratings: score=0.5, expected=0.5
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.62,
            surface="Hard",
            k=12.8,
        )
        assert server_elo == 1500.0
        assert returner_elo == 1500.0

    def test_zero_sum(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1600.0,
            returner_return_elo=1400.0,
            serve_pct=0.68,
            surface="Hard",
            k=12.8,
        )
        # Gains + losses should sum to zero
        server_delta = server_elo - 1600.0
        returner_delta = returner_elo - 1400.0
        assert abs(server_delta + returner_delta) < 1e-10

    def test_strong_server_vs_weak_returner_expected_high(self):
        # Strong server (1700) vs weak returner (1300): good serve expected
        # Serving at baseline should DECREASE server elo (underperformed expectations)
        server_elo, _ = update_serve_elo(
            server_serve_elo=1700.0,
            returner_return_elo=1300.0,
            serve_pct=0.62,
            surface="Hard",
            k=12.8,
        )
        assert server_elo < 1700.0

    def test_none_serve_pct_returns_unchanged(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=None,
            surface="Hard",
            k=12.8,
        )
        assert server_elo == 1500.0
        assert returner_elo == 1500.0

    def test_clay_baseline_different(self):
        # 0.62 on hard is baseline (score=0.5), on clay is above baseline (score>0.5)
        hard_serve, _ = update_serve_elo(1500.0, 1500.0, 0.62, "Hard", 12.8)
        clay_serve, _ = update_serve_elo(1500.0, 1500.0, 0.62, "Clay", 12.8)
        assert hard_serve == 1500.0
        assert clay_serve > 1500.0
```

**Step 2: Run tests to verify they fail**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestUpdateServeElo -v`
Expected: FAIL — `TypeError` (old signature takes different args)

**Step 3: Implement new update_serve_elo**

Replace `update_serve_elo` in `src/mvp/atptour/elo/ratings.py`:

```python
def update_serve_elo(
    server_serve_elo: float,
    returner_return_elo: float,
    serve_pct: float | None,
    surface: str,
    k: float,
) -> tuple[float, float]:
    """Update serve and return Elo for a serve sub-game.

    Uses opponent-relative Elo: normalizes serve% to a [0,1] score,
    computes expected score from serve Elo vs return Elo, and updates
    both ratings (zero-sum).

    Returns:
        Tuple of (new_server_serve_elo, new_returner_return_elo).
    """
    if serve_pct is None:
        return server_serve_elo, returner_return_elo

    score = normalize_serve_score(serve_pct, surface)
    expected = expected_score(server_serve_elo, returner_return_elo)
    surprise = score - expected

    return (
        server_serve_elo + k * surprise,
        returner_return_elo - k * surprise,
    )
```

**Step 4: Run tests to verify they pass**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestUpdateServeElo -v`
Expected: PASS (all 7 tests)

**Step 5: Commit**

```
feat(elo): rewrite update_serve_elo with opponent-relative formula
```

---

### Task 3: Rewrite update_return_elo with opponent-relative formula

**Files:**
- Modify: `src/mvp/atptour/elo/ratings.py`
- Test: `tests/atptour/elo/test_ratings.py`

**Step 1: Write the failing tests**

Replace the existing `TestUpdateReturnElo` class:

```python
class TestUpdateReturnElo:
    """Test opponent-relative return Elo update.

    Note: update_return_elo is a thin wrapper — it takes the returner's
    perspective and calls update_serve_elo with swapped roles. The opp's
    serve% is the input (the returner faced that serve).
    """

    def test_good_return_increases_return_elo(self):
        # Opponent served 0.55 on hard (below 0.62 baseline) = good returning
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=0.55,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo > 1500.0
        assert server_elo < 1500.0

    def test_poor_return_decreases_return_elo(self):
        # Opponent served 0.70 on hard (above baseline) = poor returning
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=0.70,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo < 1500.0
        assert server_elo > 1500.0

    def test_none_returns_unchanged(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=None,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo == 1500.0
        assert server_elo == 1500.0

    def test_zero_sum(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1600.0,
            server_serve_elo=1400.0,
            opp_serve_pct=0.58,
            surface="Hard",
            k=12.8,
        )
        returner_delta = returner_elo - 1600.0
        server_delta = server_elo - 1400.0
        assert abs(returner_delta + server_delta) < 1e-10
```

**Step 2: Run tests to verify they fail**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestUpdateReturnElo -v`
Expected: FAIL — `TypeError` (old signature)

**Step 3: Implement new update_return_elo**

Replace `update_return_elo` in `src/mvp/atptour/elo/ratings.py`:

```python
def update_return_elo(
    returner_return_elo: float,
    server_serve_elo: float,
    opp_serve_pct: float | None,
    surface: str,
    k: float,
) -> tuple[float, float]:
    """Update return and serve Elo for a return sub-game.

    This is the returner's perspective of a serve sub-game. The opponent's
    serve% is the input — low opp_serve_pct means the returner did well.

    Returns:
        Tuple of (new_returner_return_elo, new_server_serve_elo).
    """
    if opp_serve_pct is None:
        return returner_return_elo, server_serve_elo

    # From the server's perspective, then flip the results
    new_server, new_returner = update_serve_elo(
        server_serve_elo, returner_return_elo, opp_serve_pct, surface, k
    )
    return new_returner, new_server
```

**Step 4: Run tests to verify they pass**

Run: `poetry run python -m pytest tests/atptour/elo/test_ratings.py::TestUpdateReturnElo -v`
Expected: PASS (all 4 tests)

**Step 5: Commit**

```
feat(elo): rewrite update_return_elo with opponent-relative formula
```

---

### Task 4: Update compute.py serve/return block

**Files:**
- Modify: `src/mvp/atptour/elo/compute.py:294-309`
- Test: `tests/atptour/elo/test_compute.py`

**Step 1: Write the failing test**

Add to `tests/atptour/elo/test_compute.py`:

```python
class TestServeReturnEloUpdates:
    """Test that serve/return Elo updates both players per match."""

    def _make_match_df(self, player_serve_won, player_serve_played,
                       opp_serve_won, opp_serve_played):
        """Helper: single match with serve stats for both players."""
        return pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "player_id": ["A", "B"],
            "opp_id": ["B", "A"],
            "won": [True, False],
            "surface": ["Hard", "Hard"],
            "round": ["R32", "R32"],
            "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "player_rankings_rank": [100, 100],
            "opp_rankings_rank": [100, 100],
            "pts_service_pts_won": [player_serve_won, opp_serve_won],
            "pts_service_pts_played": [player_serve_played, opp_serve_played],
            "pts_return_pts_won": [None, None],
            "pts_return_pts_played": [None, None],
            "opp_pts_service_pts_won": [opp_serve_won, player_serve_won],
            "opp_pts_service_pts_played": [opp_serve_played, player_serve_played],
        })

    def test_both_players_serve_elo_updated(self):
        # A serves 70% (above baseline), B serves 55% (below baseline)
        df = self._make_match_df(70, 100, 55, 100)
        result = compute_elo_ratings(df)

        a_row = result.filter(pl.col("player_id") == "A")
        b_row = result.filter(pl.col("player_id") == "B")

        # After match: A's serve elo should have been updated (via sub-game 1)
        # B's serve elo should have been updated (via sub-game 2)
        # Pre-match values are both 1500 (default), so we check the NEXT match
        # to see the effect. With only one match, we need a second match.
        pass  # See test_second_match_reflects_updates

    def test_second_match_reflects_serve_updates(self):
        """Two matches: verify serve Elo changed after first match."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A", "B", "A", "C"],
            "opp_id": ["B", "A", "C", "A"],
            "won": [True, False, True, False],
            "surface": ["Hard"] * 4,
            "round": ["R32"] * 4,
            "effective_match_date": (
                [date(2024, 1, 1)] * 2 + [date(2024, 1, 2)] * 2
            ),
            "player_rankings_rank": [100] * 4,
            "opp_rankings_rank": [100] * 4,
            # Match 1: A serves 70%, B serves 55%
            # Match 2: no serve stats
            "pts_service_pts_won": [70, 55, None, None],
            "pts_service_pts_played": [100, 100, None, None],
            "pts_return_pts_won": [None] * 4,
            "pts_return_pts_played": [None] * 4,
            "opp_pts_service_pts_won": [55, 70, None, None],
            "opp_pts_service_pts_played": [100, 100, None, None],
        })
        result = compute_elo_ratings(df)

        # A's serve elo in match 2 should be > 1500 (served well in match 1)
        a_m2 = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_serve_elo"][0]
        assert a_m2 > 1500.0, f"A's serve elo should have increased: {a_m2}"

    def test_return_elo_updated_from_opp_serve(self):
        """Verify return Elo updates based on opponent's serve performance."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A", "B", "A", "C"],
            "opp_id": ["B", "A", "C", "A"],
            "won": [True, False, True, False],
            "surface": ["Hard"] * 4,
            "round": ["R32"] * 4,
            "effective_match_date": (
                [date(2024, 1, 1)] * 2 + [date(2024, 1, 2)] * 2
            ),
            "player_rankings_rank": [100] * 4,
            "opp_rankings_rank": [100] * 4,
            # Match 1: A serves 62% (baseline), B serves 55% (below baseline)
            # This means A returned well (held B to 55%)
            "pts_service_pts_won": [62, 55, None, None],
            "pts_service_pts_played": [100, 100, None, None],
            "pts_return_pts_won": [None] * 4,
            "pts_return_pts_played": [None] * 4,
            "opp_pts_service_pts_won": [55, 62, None, None],
            "opp_pts_service_pts_played": [100, 100, None, None],
        })
        result = compute_elo_ratings(df)

        # A's return elo in match 2 should be > 1500
        # (B served 55% against A = A returned well)
        a_m2 = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_return_elo"][0]
        assert a_m2 > 1500.0, f"A's return elo should have increased: {a_m2}"

    def test_serve_return_zero_sum_across_match(self):
        """Total serve Elo change across both players should be zero."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A", "B", "A", "B"],
            "opp_id": ["B", "A", "B", "A"],
            "won": [True, False, True, False],
            "surface": ["Hard"] * 4,
            "round": ["R32"] * 4,
            "effective_match_date": (
                [date(2024, 1, 1)] * 2 + [date(2024, 1, 2)] * 2
            ),
            "player_rankings_rank": [100] * 4,
            "opp_rankings_rank": [100] * 4,
            "pts_service_pts_won": [70, 55, None, None],
            "pts_service_pts_played": [100, 100, None, None],
            "pts_return_pts_won": [None] * 4,
            "pts_return_pts_played": [None] * 4,
            "opp_pts_service_pts_won": [55, 70, None, None],
            "opp_pts_service_pts_played": [100, 100, None, None],
        })
        result = compute_elo_ratings(df)

        # In match 2, check that serve elo deltas sum to zero
        a_m2_serve = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_serve_elo"][0]
        b_m2_serve = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m2")
        )["player_serve_elo"][0]
        # Both started at 1500, deltas should cancel
        a_delta = a_m2_serve - 1500.0
        b_delta = b_m2_serve - 1500.0
        assert abs(a_delta + b_delta) < 1e-6
```

**Step 2: Run tests to verify they fail**

Run: `poetry run python -m pytest tests/atptour/elo/test_compute.py::TestServeReturnEloUpdates -v`
Expected: FAIL — tests fail because compute.py still uses old one-sided update logic

**Step 3: Update serve/return block in compute.py**

Replace lines 294-309 in `src/mvp/atptour/elo/compute.py`:

```python
        # Update serve/return Elo — two sub-games per match
        # Sub-game 1: player serves, opponent returns
        serve_won = row.get("pts_service_pts_won")
        serve_played = row.get("pts_service_pts_played")
        player_serve_pct = None
        if serve_won is not None and serve_played and serve_played > 0:
            player_serve_pct = serve_won / serve_played

        player_rating.serve_elo, opp_rating.return_elo = update_serve_elo(
            player_rating.serve_elo, opp_rating.return_elo,
            player_serve_pct, surface, k_serve,
        )

        # Sub-game 2: opponent serves, player returns
        opp_serve_won = row.get("opp_pts_service_pts_won")
        opp_serve_played = row.get("opp_pts_service_pts_played")
        opp_serve_pct = None
        if opp_serve_won is not None and opp_serve_played and opp_serve_played > 0:
            opp_serve_pct = opp_serve_won / opp_serve_played

        opp_rating.serve_elo, player_rating.return_elo = update_serve_elo(
            opp_rating.serve_elo, player_rating.return_elo,
            opp_serve_pct, surface, k_serve,
        )
```

Also remove the now-unused `update_return_elo` import from compute.py if desired
(it's only used from ratings.py internally now). Or keep it — it's still a public
function.

**Step 4: Run tests to verify they pass**

Run: `poetry run python -m pytest tests/atptour/elo/ -v`
Expected: ALL PASS

**Step 5: Commit**

```
feat(elo): update compute.py to use two-sided serve/return Elo updates
```

---

### Task 5: Fix any broken existing tests

**Files:**
- Modify: `tests/atptour/elo/test_ratings.py` (existing tests that call old signatures)
- Modify: `tests/atptour/elo/test_compute.py` (if existing tests need opp_pts columns)

**Step 1: Run the full Elo test suite**

Run: `poetry run python -m pytest tests/atptour/elo/ -v`

Check for failures from:
- Tests calling `update_serve_elo(current_elo, serve_pct, surface, k)` (old 4-arg signature)
- Tests calling `update_return_elo(current_elo, return_pct, surface, k)` (old 4-arg signature)
- Compute tests missing `opp_pts_service_pts_won` / `opp_pts_service_pts_played` columns
- Convergence/bounds tests in TestEloConvergence and TestEloBounds classes

**Step 2: Fix each failure**

For old-signature callers: update to new signature (5 args, returns tuple).
For missing columns in compute test DataFrames: add `opp_pts_service_pts_won` and
`opp_pts_service_pts_played` columns (can be None if not testing serve/return).

**Step 3: Run full test suite**

Run: `poetry run python -m pytest tests/atptour/elo/ -v`
Expected: ALL PASS

**Step 4: Run the full project test suite**

Run: `poetry run python -m pytest --timeout=60 -x -q`
Expected: ALL PASS (no downstream breakage since output column names unchanged)

**Step 5: Commit**

```
fix(elo): update existing tests for new serve/return Elo signatures
```

---

### Task 6: Validate with model run and Elo leaderboards

**Step 1: Re-aggregate and run model**

Run: `poetry run python -m mvp model cmp_3elo_age_2020 --refresh`

Capture: accuracy, AUC, log loss, calibration, high-conf error rate.
Compare against baseline in `docs/elo-warmup-comparison.md`.

**Step 2: Run Elo leaderboard snapshot**

Query matches.parquet for top 25 by player_serve_elo and player_return_elo
(same script pattern as used in the warm-up comparison).

Check:
- Serve Elo top 25 should be dominated by known big servers (Sinner, Raonic, Isner, Opelka, MPP, etc.)
- Return Elo top 25 should be dominated by known elite returners (Djokovic, Nadal, Alcaraz, etc.)
- No obscure low-volume players in the top 10

**Step 3: Update comparison doc**

Add results to `docs/elo-warmup-comparison.md` as a new section.

**Step 4: Commit**

```
docs: add serve/return Elo redesign validation results
```
