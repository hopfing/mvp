from dataclasses import dataclass

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

# K-factor by tournament level
TOURNAMENT_IMPORTANCE = {
    "GS": 1.2,
    "1000": 1.1,
    "500": 1.05,
    "250": 1.0,
    "CH175": 0.90,
    "CH125": 0.80,
    "CH100": 0.75,
    "CH75": 0.65,
    "CH50": 0.55,
    "FU": 0.5,
}

# Surface K-factor (more stable than overall)
SURFACE_K_MULT = 0.3

# Indoor venue K-factor (mirrors surface — an opponent-adjusted Elo modifier)
INDOOR_K_MULT = 0.3

# Serve/Return K-factor
SERVE_RETURN_K_MULT = 1.0

# RD decay/growth
RD_DECAY_FACTOR = 0.95
RD_GROWTH_PER_DAY = 0.5

# Serve/return baselines by surface
SERVE_BASELINE = {"Hard": 0.62, "Clay": 0.60, "Grass": 0.64}

# Indoor courts play faster and serve% runs higher on them. Measured on singles
# with SURFACE HELD FIXED: indoor-hard 0.6368 (n=73,746) against outdoor-hard
# 0.6243 (n=163,033), so +1.25pp.
#
# Holding surface fixed is load-bearing, not fussiness. Pooling all surfaces
# gives +2.25pp — nearly double — because "outdoor" then includes clay, whose
# own baseline is 2pp lower, so the comparison charges the clay/hard difference
# to indoor. Building on that figure would bake a ~1pp phantom effect into every
# indoor player's rating.
#
# Applied to Hard only. Indoor clay and indoor grass barely exist at tour and
# challenger level, so neither supports its own measured baseline — the same
# reason Carpet gets no surface adjustment at all.
INDOOR_SERVE_BOOST = 0.0125

# Step size for the serve/return surface and indoor adjustments, as a multiple
# of the sub-game's K. Small for the same reason SURFACE_K_MULT is: these are
# residuals on top of an already-fitted rating, learned from a fraction of a
# player's matches, and they revert toward zero between updates.
#
# One shared value across Hard/Clay/Grass/indoor-hard rather than per-axis
# constants. A split-half reliability check does suggest an ordering — the
# optimal static blend weight on surface-specific history ran grass 0.45,
# hard/clay 0.35, indoor 0.20 — but a static blend weight over complete history
# is not the same quantity as the step size of a process accumulating from zero,
# and treating it as one is the mistake that made an earlier K sweep look
# monotone forever. Differentiate only if measurement says the shared value is
# insufficient.
SERVE_SURFACE_K_MULT = 0.3
RETURN_BASELINE = {"Hard": 0.38, "Clay": 0.40, "Grass": 0.36}

# Fraction of the rank-based Elo seed carried into serve/return Elo. 1.0 = the
# full seed, no shrinkage.
#
# Started at 0.5 on the theory that rank measures overall strength, so a full
# transplant would import more general skill than serving evidence supports.
# Measured over 0.25/0.5/0.75/1.0, and the theory lost: correlation between the
# rating's implied serve% and the observed one rises monotonically with the
# seed (0.4176 / 0.4258 / 0.4308 / 0.4331), with gains decelerating toward 1.0.
#
# Judged on correlation rather than calibration or MAE deliberately. Serve Elo
# reaches the downstream model as a FEATURE of a fitted classifier, which
# absorbs a feature's scale into its own coefficient — and trees are invariant
# to monotone transforms outright — so spread-sensitive metrics score a defect
# that consumer is immune to. Correlation is what survives the handoff.
#
# The original objection was half right, and the half that held is worth
# keeping in view: the gain concentrates where rank is a validated signal.
# Tour/challenger-native players gain +0.0180 against +0.0043 for ITF
# graduates, whose rank was built almost entirely on matches carrying no serve
# statistics. Still positive there, so the seed is not harmful for them — just
# far less informative.
SERVE_SEED_SHRINK = 1.0

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

# Score normalization for serve/return Elo — logistic slope, in serve-percentage
# points, on the deviation from the surface baseline.
#
# Replaces a linear ramp with a hard clip. That ramp reached 0 and 1 at only
# +/-5pp from baseline despite a comment claiming +/-10pp (it divided by the full
# width instead of the half width), and 59.8% of singles matches landed on an
# endpoint — for three matches in five the update discarded how far from
# baseline the performance was and behaved like a binary win/loss, which is the
# property serve Elo exists to avoid. A logistic map never saturates, so a
# 15pp-over performance always moves the rating further than a 5pp-over one.
#
# 0.05 is set so the slope at the centre (0.25 / s) matches a linear ramp of
# +/-10pp, keeping near-baseline sensitivity where the previous retune put it
# while cutting the share of matches treated as maximal from 59.8% to ~5%.
# Not a free parameter: it is calibrated jointly with SERVE_RETURN_K_MULT.
SERVE_SCORE_SLOPE = 0.05

# Mean reversion — counteracts inflation from player turnover
# 1% pull per match toward DEFAULT_ELO for base Elo, toward 0 for surface adjs
REVERSION_RATE = 0.005


@dataclass(frozen=True)
class ServeEloConfig:
    """The serve/return knobs a sweep varies, bundled so a run can override them.

    Exists because the obvious alternative — patching module constants at
    runtime — fails SILENTLY. `elo/ratings.py` and `ratings/compute.py` both do
    `from ...constants import NAME`, which binds a fresh name in each importing
    module at load time. Patching `constants.py` afterwards leaves those
    bindings on the old values, so a sweep cell would score the previous
    configuration under the new label and nothing would raise. Threading the
    values explicitly inverts that failure mode: forget to pass one through and
    you get a TypeError at the call site, before any number is trusted.

    Field defaults are the constants above, so every existing caller — the live
    pipeline included — behaves exactly as it did with no config passed.
    """

    k_mult: float = SERVE_RETURN_K_MULT
    seed_shrink: float = SERVE_SEED_SHRINK
    score_slope: float = SERVE_SCORE_SLOPE

    def stamp(self) -> dict[str, float]:
        """The resolved values, for recording alongside a run's output.

        Read off the instance actually used, never re-derived from the module
        constants — re-deriving would reintroduce the ambiguity about which
        source was authoritative, which is the thing this class removes.
        """
        return {
            "k_mult": self.k_mult,
            "seed_shrink": self.seed_shrink,
            "score_slope": self.score_slope,
        }


DEFAULT_SERVE_ELO_CONFIG = ServeEloConfig()
