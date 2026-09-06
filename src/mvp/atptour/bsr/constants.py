"""Hyperparameters and fixed effects of the serve/return skill filter.

Every number here was produced by `scripts/bsr/write_bsr_constants.py` from
the probe's tune (`scripts/bsr/probe_bsr.py --stage tune --seed-mode elo
--disp-mode re`, 150 trials, objective = prequential binomial log-likelihood
on 2016-2021 observations) and its fixed-effect fit on the 2015-2021 span.
Nothing here is read from B:/ at runtime; re-tuning means re-running the
script and pasting its output.

Bundled as a frozen dataclass for the reason `ServeEloConfig` gives: the
ratings pass binds names at import time, so the only override that cannot
fail silently is an instance threaded through explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# Surface / circuit indices shared by the tracker and the mu-cell table.
# Anything not Hard/Clay/Grass (Carpet, null) maps to Hard, as the probe did.
SURFACE_INDEX = {"Hard": 0, "Clay": 1, "Grass": 2}
CIRCUIT_INDEX = {"tour": 0, "chal": 1}

# Fixed effects mu(surface, circuit, indoor) on the logit scale, indexed
# surface*4 + circuit*2 + indoor. Fit span 2015-01-01..2021-12-31, 151,204
# server-match observations (tour + chal singles). Indoor clay cells have
# their own fit; indoor grass has no observations and carries the outdoor
# grass value.
MU_CELLS: tuple[float, ...] = (
    0.5525381237, 0.5691292323, 0.4733633823, 0.5478813589,
    0.4693170868, 0.5119588671, 0.3864949927, 0.3848556184,
    0.6426384918, 0.6426384918, 0.6188961169, 0.6188961169,
)


@dataclass(frozen=True)
class BsrConfig:
    """The filter's knobs. Field defaults are the tuned values."""

    # Random-walk variance per day on the overall serve / return axes; the
    # elapsed-day count is capped so a long absence does not blow the prior.
    q_s: float = 2.1859571733982455e-05
    q_r: float = 2.0316890516025758e-05
    cap_days: float = 304.2028723074866
    # Surface residual axes: AR(1) toward zero, applied once per observation on
    # that surface (the cadence measured as best), with this innovation.
    q_surf: float = 0.0004286173606103864
    phi_surf: float = 0.8354292615504447
    # Prior variance of a new player's overall axes.
    v0: float = 0.062307747202284006
    # Cold start: prior mean = seed * (pre-match serve/return Elo - 1500) / 100.
    seed_es: float = 0.3279483258118909
    seed_er: float = 0.2539311419400611
    # Per-observation random effect on eta (match-day form). Tuned per circuit
    # (0.0265 tour, 0.0369 chal); shipped tied to the tour value, which the
    # selection span measured as no loss.
    tau2: float = 0.026544910565251066
    newton: int = 2
    # Observations before this date are outside the tuning domain and are not
    # applied; state is emitted for them if it exists.
    start_date: date = date(2015, 1, 1)
    mu_cells: tuple[float, ...] = MU_CELLS

    def stamp(self) -> dict[str, float]:
        return {
            "q_s": self.q_s, "q_r": self.q_r, "cap_days": self.cap_days,
            "q_surf": self.q_surf, "phi_surf": self.phi_surf, "v0": self.v0,
            "seed_es": self.seed_es, "seed_er": self.seed_er, "tau2": self.tau2,
            "newton": float(self.newton),
        }


DEFAULT_BSR_CONFIG = BsrConfig()
