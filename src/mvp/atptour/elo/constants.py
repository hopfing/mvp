# Starting values
DEFAULT_ELO = 1500.0
DEFAULT_RD = 350.0
MIN_RD = 50.0
MAX_RD = 350.0

# K-factor settings
BASE_K = 32.0
NEW_PLAYER_K_MULT = 1.5
HIGH_RD_K_MULT = 1.2
HIGH_RD_THRESHOLD = 200.0
NEW_PLAYER_THRESHOLD = 30

# K-factor by round importance
ROUND_IMPORTANCE = {
    "F": 1.3,
    "SF": 1.2,
    "QF": 1.1,
    "R16": 1.0,
    "R32": 1.0,
    "R64": 0.95,
    "R128": 0.9,
    "Q1": 0.85,
    "Q2": 0.85,
    "Q3": 0.85,
    "RR": 1.0,
}

# Surface K-factor (more stable than overall)
SURFACE_K_MULT = 0.5

# Serve/Return K-factor
SERVE_RETURN_K_MULT = 0.4

# RD decay/growth
RD_DECAY_FACTOR = 0.95
RD_GROWTH_PER_DAY = 0.5

# Serve/return baselines by surface
SERVE_BASELINE = {"Hard": 0.62, "Clay": 0.60, "Grass": 0.64}
RETURN_BASELINE = {"Hard": 0.38, "Clay": 0.40, "Grass": 0.36}

# Serve/return update scaling
SERVE_RETURN_SCALE = 400.0

# Initial seeding from ranking
SEED_ELO_MAX = 2400.0
SEED_ELO_MIN = 1200.0
SEED_UNRANKED = 1300.0
SEED_RANK_COEFF = 40.0
