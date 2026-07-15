# System constant - controls volatility change rate
# Lower = more stable sigma (good for single-game rating periods)
TAU = 0.5

# Initial values (all players start equal)
INITIAL_MU = 1500.0
INITIAL_RD = 350.0
INITIAL_SIGMA = 0.06

# Bounds
MIN_RD = 30.0
MAX_RD = 350.0
MIN_SIGMA = 0.01
MAX_SIGMA = 0.15

# Glicko-2 scale factor: 400 / ln(10)
SCALE = 173.7178

# Convergence tolerance for Illinois method
EPSILON = 1e-6

# Mean reversion on mu — counteracts the non-conservation of the RD-weighted
# asymmetric mu update under player turnover (new players enter below the pool
# level and donate rating upward as they climb; see ratings/compute.py). Mirrors
# the Elo reversion (elo/constants.py REVERSION_RATE) but is its OWN constant,
# NOT copied from Elo. RD-scaled at apply time, so uncertain (new/returning)
# players revert most and converged players barely. Validated on the full real
# singles history (scripts/glicko_rerun_ratings.py + glicko_validate_ratings.py):
# 0.004 flattens modern-era mu drift (1990+: +380 -> -3) while keeping glicko_diff
# calibration pristine and discrimination intact; 0.008 over-reverts and distorts
# the diff across match-count gaps. No-regression backtest confirmed at deploy.
GLICKO_REVERSION_RATE = 0.004
