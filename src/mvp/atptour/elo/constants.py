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
SERVE_RETURN_K_MULT = 1.0

# RD decay/growth
RD_DECAY_FACTOR = 0.95
RD_GROWTH_PER_DAY = 0.5

# Serve/return baselines by surface
SERVE_BASELINE = {"Hard": 0.62, "Clay": 0.60, "Grass": 0.64}
RETURN_BASELINE = {"Hard": 0.38, "Clay": 0.40, "Grass": 0.36}

# Serve/return update scaling
SERVE_RETURN_SCALE = 4000.0

# Initial seeding from ranking
SEED_ELO_MAX = 2400.0
SEED_ELO_MIN = 1200.0
SEED_UNRANKED = 1300.0
SEED_RANK_COEFF = 40.0

# Style dimension baselines (calculated from historical data)
# First serve power = aces / first_serve_pts_won
FIRST_SERVE_POWER_BASELINE = {"Hard": 0.176, "Clay": 0.110, "Grass": 0.198}

# Second serve reliability = 1 - (DFs / second_serve_pts_played)
SECOND_SERVE_RELIABILITY_BASELINE = {"Hard": 0.893, "Clay": 0.895, "Grass": 0.896}

# Ace resistance = 1 - (opp_aces / return_first_serve_pts_lost)
ACE_RESISTANCE_BASELINE = {"Hard": 0.824, "Clay": 0.890, "Grass": 0.802}

# Serve clutch = bp_saved / bp_faced
SERVE_CLUTCH_BASELINE = {"Hard": 0.597, "Clay": 0.575, "Grass": 0.627}

# Return clutch = bp_converted / bp_opportunities
RETURN_CLUTCH_BASELINE = {"Hard": 0.404, "Clay": 0.425, "Grass": 0.373}

# TB clutch = tiebreak win rate (zero-sum, surface-agnostic)
TB_CLUTCH_BASELINE = 0.50

# Style dimension update settings
STYLE_K_MULT = 0.3  # More conservative than serve/return (0.4)
STYLE_SCALE = 3000.0  # Smaller scale than serve/return (4000)

# EMA smoothing for serve/return Elo and style dimensions
EMA_ALPHA = 0.10  # Half-life ~7 matches

# Indoor adjustment EMA scale (centered at 0, not DEFAULT_ELO)
INDOOR_EMA_SCALE = 500.0

# Score normalization for serve/return Elo
# Maps ±10pp from surface baseline to the full [0,1] range
SERVE_RETURN_DEVIATION_SCALE = 0.10

# Mean reversion — counteracts inflation from player turnover
# 1% pull per match toward DEFAULT_ELO for base Elo, toward 0 for surface adjs
REVERSION_RATE = 0.005
