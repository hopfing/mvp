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
