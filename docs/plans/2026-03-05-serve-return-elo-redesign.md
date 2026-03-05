# Serve/Return Elo Redesign

## Problem

The current serve and return "Elo" ratings are not actually Elo. They are
opponent-agnostic EMA performance trackers that:

1. Don't account for opponent strength — serving 65% against Djokovic produces
   the same update as serving 65% against a qualifier
2. Only update one player per match — the opponent's serve/return Elo is not
   updated from the match
3. Use EMA toward a fixed baseline rather than the Elo expected-score formula

This produces broken leaderboards (Chris Guccione #1 serve Elo, Andrei Gorban #1
return Elo) and noisy ratings for low-volume players. The date cutoff experiment
confirmed that even with clean 2020+ data, return Elo is still dominated by
obscure players.

## Design

Replace serve/return EMA with true opponent-relative Elo using the standard
logistic formula.

### Core Formula

Each match with stats produces two sub-games:

**Sub-game 1: Player A serves, Player B returns**

```
score = clamp((A_serve_pct - serve_baseline[surface]) / DEVIATION_SCALE + 0.5, 0, 1)
expected = 1 / (1 + 10^((B.return_elo - A.serve_elo) / 400))
surprise = score - expected
A.serve_elo  += K_serve * surprise
B.return_elo -= K_serve * surprise
```

**Sub-game 2: Player B serves, Player A returns** — same logic, reversed.

### Score Normalization

Raw serve% (~55-70%) is normalized to a [0,1] score centered at 0.5 for
baseline performance:

- `SERVE_RETURN_DEVIATION_SCALE = 0.20`
- ±10 percentage points from surface baseline maps to the full [0,1] range
- Examples on hard court (baseline 62%):
  - 62% → score 0.50 (average, no surprise)
  - 67% → score 0.75 (good)
  - 72% → score 1.00 (elite, clamped)
  - 52% → score 0.00 (terrible, clamped)

### Per-Match Updates

One match updates four ratings:
- Player's serve Elo (sub-game 1)
- Opponent's return Elo (sub-game 1)
- Opponent's serve Elo (sub-game 2)
- Player's return Elo (sub-game 2)

Both sub-games are zero-sum: the server's gain equals the returner's loss.

### Data Inputs

- `pts_service_pts_won / pts_service_pts_played` — player's serve%
- `opp_pts_service_pts_won / opp_pts_service_pts_played` — opponent's serve%
- Both available on each row (no structural change needed)

### What Changes

- `update_serve_elo()` in ratings.py — new opponent-relative formula
- `update_return_elo()` in ratings.py — new opponent-relative formula
- Serve/return update block in compute.py — two sub-games, both players updated
- New constant: `SERVE_RETURN_DEVIATION_SCALE = 0.20`
- Remove unused `k` parameter from update_serve_elo/update_return_elo signatures

### What Stays

- `PlayerRating` dataclass — same fields
- Output column names — same, no downstream impact
- RD tracking for serve/return — same decay/growth/inactivity logic
- K-factor system — dynamic, `SERVE_RETURN_K_MULT = 0.4`
- Initial serve/return ratings — 1500 (not seeded from ranking)
- Surface baselines — `SERVE_BASELINE`, `RETURN_BASELINE` already in constants.py
- Style dimensions — completely untouched
- Downstream features (`svc_elo_diff`, `ret_elo_diff`) and model config

### Known Compromise

RD decays after every match regardless of whether serve/return stats were
available. This means uncertainty decreases even when the rating wasn't updated.
Correct behavior would track "last match with stats" separately, but this adds
complexity. Revisit if results are noisy after the rework.

## Validation

Run `cmp_3elo_age_2020` model before and after. Compare:
- Headline metrics (accuracy, AUC, log loss, calibration)
- High-confidence error rate
- Top-25 serve/return Elo leaderboards (sanity check)

Baseline already captured in `docs/elo-warmup-comparison.md`.
