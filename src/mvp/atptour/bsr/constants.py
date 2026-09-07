"""Hyperparameters and fixed effects of the serve/return skill filter.

Every number for the shipped `serve` stream was produced by
`scripts/bsr/write_bsr_constants.py` from the probe's tune
(`scripts/bsr/probe_bsr.py --stage tune --seed-mode elo --disp-mode re`, 150
trials, objective = prequential binomial log-likelihood on 2016-2021
observations) and its fixed-effect fit on the 2015-2021 span. Nothing here is
read from B:/ at runtime; re-tuning means re-running the script and pasting its
output.

Bundled as a frozen dataclass for the reason `ServeEloConfig` gives: the
ratings pass binds names at import time, so the only override that cannot
fail silently is an instance threaded through explicitly.

## The 21 streams

`BsrConfig.streams` carries one `StreamConfig` per binomial observation stream,
in a FIXED order with the shipped `serve` stream at index 0. The order is the
contract between three places that must agree: the tracker's per-stream state
lists, the count arrays the ratings pass builds, and the column slice each row
writes into the output slab. Never reorder; append.

Each stream names the counts it observes as `k_terms` / `n_terms` — signed sums
of PLAYER-side aggregate columns, mirrored to the opponent by `mirror_col`. Two
streams (`tb`, `tbpts`) derive their counts from the per-set tiebreak scores
instead and carry a `derived` tag; the ratings pass special-cases those.

Orientation: every stream's k is the server-favourable (or player-favourable)
event, so one sign convention serves the whole Elo-seeded prior. `df` is
therefore "second serves NOT double-faulted", and `hardhold` / `deuce`, whose k
is not favourable, take no Elo seed.
"""

from __future__ import annotations

import dataclasses as _dc
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
# grass value. This is the SHIPPED `serve` stream's table; the other streams
# carry their own under `StreamConfig.mu_cells`.
MU_CELLS: tuple[float, ...] = (
    0.5525381237, 0.5691292323, 0.4733633823, 0.5478813589,
    0.4693170868, 0.5119588671, 0.3864949927, 0.3848556184,
    0.6426384918, 0.6426384918, 0.6188961169, 0.6188961169,
)

# The shipped stream's tuned knobs, named once so `BsrConfig`'s scalar fields
# (which the probe-parity test and the verify script read) and the `serve`
# StreamConfig cannot drift apart.
_Q_S = 2.1859571733982455e-05
_Q_R = 2.0316890516025758e-05
_CAP_DAYS = 304.2028723074866
_Q_SURF = 0.0004286173606103864
_PHI_SURF = 0.8354292615504447
_V0 = 0.062307747202284006
_SEED_ES = 0.3279483258118909
_SEED_ER = 0.2539311419400611
_TAU2 = 0.026544910565251066

_ZERO_CELLS: tuple[float, ...] = (0.0,) * 12


def mirror_col(name: str) -> str:
    """The opponent's column for a player-side aggregate column.

    Three naming conventions coexist in the aggregate and all three are here:
    `svc_/pts_/ret_` prefix with a bare `opp_`, the match-box feed's
    `mb_player_` -> `mb_opp_`, and the stats-plus / rally / stroke feeds'
    `player_` -> `opp_`.
    """
    if name.startswith("mb_player_"):
        return "mb_opp_" + name[len("mb_player_"):]
    if name.startswith("player_"):
        return "opp_" + name[len("player_"):]
    return "opp_" + name


@dataclass(frozen=True)
class StreamConfig:
    """One binomial observation stream: what it observes and its knobs.

    `k_terms` / `n_terms` are signed sums of player-side count columns. A
    stream whose `derived` tag is set takes its counts from the ratings pass
    instead (the per-set tiebreak scores) and leaves both empty.

    `has_returner` gives the stream a second axis carried by the OTHER player.
    On the serve streams that axis is a return skill. On the rally, stroke,
    net and winners streams it is the opponent's own suppression of the
    player's ratio, NOT a return effect — the rally transformer attributes
    each point to whoever ended it, so `k / n` is a winner-to-error ratio
    among the points the player ended, not a share of points won.
    """

    name: str
    k_terms: tuple[tuple[int, str], ...]
    n_terms: tuple[tuple[int, str], ...]
    has_returner: bool
    has_surface: bool
    has_indoor: bool
    # Which `_capture_elo_values` key seeds a new player's prior mean on each
    # axis, or None for a flat-zero seed. All six named components and both
    # base serve/return Elos are DEFAULT_ELO(1500)-centred, so the prior mean
    # is `seed * (elo - 1500) / 100` for every one of them.
    seed_src_s: str | None
    seed_src_r: str | None
    q_s: float
    q_r: float
    q_surf: float
    q_indoor: float
    v0: float
    seed_s: float
    seed_r: float
    tau2: float
    mu_cells: tuple[float, ...]
    derived: str | None = None
    # Emission trims (2026-09-06 audit on 134k in-domain rows since 2022):
    # a stream's observation count and days-since clock are emitted only
    # where its observation set is its own (one clock per feed); the
    # surface/indoor residual SPREADS are emitted only on the pooled stream,
    # because a residual's variance depends on the observation history alone
    # and the streams sharing a box score share it to four decimals.
    emit_counts: bool = True
    emit_residual_sd: bool = True

    def input_columns(self) -> tuple[str, ...]:
        """Player-side columns this stream reads, in no particular order."""
        return tuple(
            dict.fromkeys(c for _, c in self.k_terms + self.n_terms)
        )


def _stream(
    name: str,
    k_terms: tuple[tuple[int, str], ...] = (),
    n_terms: tuple[tuple[int, str], ...] = (),
    *,
    has_returner: bool = False,
    has_surface: bool = False,
    has_indoor: bool = False,
    seed_src_s: str | None = None,
    seed_src_r: str | None = None,
    derived: str | None = None,
    emit_counts: bool = True,
    emit_residual_sd: bool = True,
    q_s: float = _Q_S,
    q_r: float = _Q_R,
    q_surf: float = _Q_SURF,
    q_indoor: float = _Q_SURF,
    v0: float = _V0,
    seed_s: float = 0.0,
    seed_r: float = 0.0,
    tau2: float = _TAU2,
    mu_cells: tuple[float, ...] = _ZERO_CELLS,
) -> StreamConfig:
    return StreamConfig(
        name=name, k_terms=k_terms, n_terms=n_terms,
        has_returner=has_returner, has_surface=has_surface,
        has_indoor=has_indoor, seed_src_s=seed_src_s, seed_src_r=seed_src_r,
        q_s=q_s, q_r=q_r, q_surf=q_surf, q_indoor=q_indoor, v0=v0,
        seed_s=seed_s, seed_r=seed_r, tau2=tau2, mu_cells=mu_cells,
        derived=derived,
        emit_counts=emit_counts,
        emit_residual_sd=emit_residual_sd,
    )


def _shot(name: str, col: str, **knobs) -> StreamConfig:
    """A stroke stream: winners over winners + forced + unforced.

    One definition for every stroke. `{col}_others` is never a denominator —
    it dominates the rare strokes (lob: 4.9 of 5.5 shots) and folding it in
    would make the rate a share of shots hit rather than of outcomes.

    Only `fh` and `bh` survive. The plan's other six stroke families —
    volley, passing, approach, drop_shot, lob, overhead — were dropped by the
    user after the probe measured that none of them has an observation before
    2024: their nominal 2022-2023 fit span is empty, and every observation
    they do have falls inside or after the 2024-2025 eval span, so fitting
    them at all means fitting on data the eval will score. Their tune
    artefacts are kept under B:/research/bsr_ms/_dropped/ if that is ever
    revisited.
    """
    return _stream(
        name,
        ((1, f"player_{col}_winners"),),
        (
            (1, f"player_{col}_winners"),
            (1, f"player_{col}_forced_errors"),
            (1, f"player_{col}_unforced_errors"),
        ),
        has_returner=True,
        **knobs,
    )


def _rally(name: str, col: str, **knobs) -> StreamConfig:
    """A rally-length stream: the winner-to-error ratio among the points the
    player ended at that rally length (see StreamConfig.has_returner)."""
    return _stream(
        name,
        ((1, f"player_{col}_won"),),
        ((1, f"player_{col}_won"), (1, f"player_{col}_err")),
        has_returner=True,
        **knobs,
    )


# ---------------------------------------------------------------------------
# The stream table. ORDER IS A CONTRACT (see the module docstring).
#
# Every knob and every mu cell below is pasted from
# `poetry run py scripts/bsr/write_bsr_constants.py --multi`, which reads
# B:/research/bsr_ms/best_all.json (200 Optuna trials per stream) and refits
# the 12 mu cells with the probe's own loader. `cap_days`, `phi_surf` and
# `newton` are shared across every stream and stay at the shipped values; the
# indoor residual axis reuses `phi_surf`. A stream whose median n < 10 has its
# tau2 tied to the shipped pooled value and was excluded from that study.
# The `serve` entry is the one exception: it keeps its SHIPPED literals (see
# the comment on it below), taking only `q_indoor` from the multi-stream tune.
# ---------------------------------------------------------------------------
STREAMS: tuple[StreamConfig, ...] = (
    # -- the shipped stream; every value below is the tuned/fitted one --
    # `serve` carries the indoor residual axis by decision, though it emits no
    # column for it: the twelve shipped names stay exactly as they are and the
    # axis changes their VALUES. The measurement behind the decision is that
    # the axis is worth about -0.00003 LL/pt on the tune span, i.e. nothing,
    # and the cost is that the shipped columns move; both were accepted
    # because the three serve FS runs are being redone with the new columns
    # anyway. `has_indoor=False` here plus the shipped literals reproduces the
    # old filter exactly, which is what the port's regression tests assert.
    # Its seven shipped knobs and mu cells are unchanged, so the twelve
    # columns stay byte-identical on every OUTDOOR row; `q_indoor` is the one
    # value the new axis needs and comes from the probe's with-indoor tune.
    _stream(
        "serve",
        ((1, "pts_service_pts_won"),),
        ((1, "pts_service_pts_played"),),
        has_returner=True, has_surface=True, has_indoor=True,
        seed_src_s="serve_elo", seed_src_r="return_elo",
        seed_s=_SEED_ES, seed_r=_SEED_ER, mu_cells=MU_CELLS,
        q_indoor=0.0018988169886704125,
    ),
    # -- the three chain components the two-level model actually fits on --
    _stream(
        "fsi",
        ((1, "svc_first_serve_in"),),
        ((1, "pts_service_pts_played"),),
        has_surface=True, has_indoor=True,
        q_s=1.4058660055789087e-05,
        q_r=0.0,
        q_surf=0.004264418359587366,
        q_indoor=5.168428649082841e-05,
        v0=0.03831555410938921,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.014547784437498663,
        mu_cells=(
            0.4248812282, 0.4466880599, 0.4175514520, 0.4540349151,
            0.4870012934, 0.4585991826, 0.4376382250, 0.4563000229,
            0.5267709421, 0.4451034004, 0.5553815560, 0.4451034004,
        ),
    ),
    _stream(
        "w1",
        ((1, "svc_first_serve_pts_won"),),
        ((1, "svc_first_serve_in"),),
        has_returner=True, has_surface=True, has_indoor=True,
        seed_src_s="serve_elo", seed_src_r="return_elo",
        q_s=3.7854156159318875e-05,
        q_r=1.366527163746324e-05,
        q_surf=0.0003564072209689272,
        q_indoor=0.002802686473262052,
        v0=0.05892387152690654,
        seed_s=0.2746012409900017,
        seed_r=0.19308872896072046,
        tau2=0.036776185540241084,
        mu_cells=(
            0.9394974989, 0.9668052097, 0.8253451796, 0.9391223869,
            0.7752339057, 0.8592145853, 0.6878592319, 0.6756401175,
            1.0317995469, 0.8298056548, 0.9821446718, 0.8298056548,
        ),
    ),
    _stream(
        "w2",
        ((1, "svc_second_serve_pts_won"),),
        ((1, "svc_second_serve_pts_played"),),
        has_returner=True, has_surface=True, has_indoor=True,
        seed_src_s="second_serve_reliability", seed_src_r="return_elo",
        q_s=1.0272663878337509e-05,
        q_r=4.94943007410232e-06,
        q_surf=0.00023202368649506184,
        q_indoor=4.605044877398204e-05,
        v0=0.05216413960792168,
        seed_s=0.19631229790522525,
        seed_r=0.08285450948524471,
        tau2=0.016598103427368612,
        mu_cells=(
            0.0232711273, 0.0170298857, -0.0162023940, -0.0025617059,
            0.0089180217, 0.0123197913, -0.0523116769, -0.0473882696,
            0.0665397569, -0.0096824687, 0.0592027152, -0.0096824687,
        ),
    ),
    # -- auxiliary serve-side rates; no surface axes (their surface
    #    dependence rides on the component skills above) --
    _stream(
        "ace",
        ((1, "svc_aces"),),
        ((1, "pts_service_pts_played"),),
        has_returner=True,
        seed_src_s="first_serve_power", seed_src_r="ace_resistance",
        q_s=0.00023123729637584587,
        q_r=3.8417718074107e-05,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.16027899978278223,
        seed_s=0.46130496218091604,
        seed_r=0.06533376970916327,
        tau2=0.11219239749351469,
        mu_cells=(
            -2.3941189281, -2.3806214492, -2.6418565238, -2.3962192438,
            -2.8937738523, -2.7335393924, -3.1015352109, -3.0827119644,
            -2.2619732046, -2.6565597850, -2.3585011472, -2.6565597850,
        ),
    ),
    _stream(
        # Orientation: second serves NOT double-faulted, so k is favourable.
        "df",
        ((1, "svc_second_serve_pts_played"), (-1, "svc_double_faults")),
        ((1, "svc_second_serve_pts_played"),),
        seed_src_s="second_serve_reliability",
        q_s=9.146896723470498e-05,
        q_r=0.0,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.145143719424173,
        seed_s=0.32047872148818474,
        seed_r=0.0,
        tau2=0.058991383806085554,
        mu_cells=(
            2.1300790496, 2.2786770081, 2.0244205202, 2.1237339409,
            2.2829004681, 2.2470555890, 2.0896365751, 2.3177332956,
            2.1944135989, 2.1202650175, 2.0840151945, 2.1202650175,
        ),
    ),
    _stream(
        "bp",
        ((1, "svc_bp_saved"),),
        ((1, "svc_bp_faced"),),
        has_returner=True,
        seed_src_s="serve_clutch", seed_src_r="return_clutch",
        q_s=5.265680514959164e-06,
        q_r=1.433583241071861e-06,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.013747880700660917,
        seed_s=0.015658448702932856,
        seed_r=0.023120333579962394,
        tau2=0.026544910565251066,
        mu_cells=(
            0.4347063866, 0.4524177721, 0.3494444618, 0.4205363944,
            0.3848348547, 0.3953314424, 0.2852518299, 0.3174586071,
            0.5329646681, 0.3665257505, 0.4931629544, 0.3665257505,
        ),
    ),
    # -- full-history streams from the score and the per-set tiebreaks --
    _stream(
        # Games held = games served minus breaks conceded.
        "hold",
        ((1, "svc_games_played"), (-1, "svc_bp_faced"), (1, "svc_bp_saved")),
        ((1, "svc_games_played"),),
        has_returner=True, has_surface=True, has_indoor=True,
        seed_src_s="serve_elo", seed_src_r="return_elo",
        q_s=8.600170290402003e-05,
        q_r=5.261764120009311e-05,
        q_surf=0.009300599177403923,
        q_indoor=0.00017063965637928124,
        v0=0.3571097114247016,
        seed_s=0.5302371102001939,
        seed_r=0.33968753003009167,
        tau2=0.12711552977973659,
        mu_cells=(
            1.3135163053, 1.3594020065, 1.1184244027, 1.3029260439,
            1.1178320168, 1.2152744193, 0.9119046237, 0.9247884407,
            1.5460478655, 1.1431409093, 1.4784218080, 1.1431409093,
        ),
    ),
    _stream(
        "tb", has_returner=True, derived="tb",
        seed_src_s="tb_clutch", seed_src_r="tb_clutch",
        q_s=5.927775151296177e-06,
        q_r=1.193093562112232e-05,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.037105521013273045,
        seed_s=0.02437407562274649,
        seed_r=0.010212651890891992,
        tau2=0.026544910565251066,
        mu_cells=(
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
        ),
    ),
    _stream(
        "tbpts", has_returner=True, derived="tbpts",
        seed_src_s="tb_clutch", seed_src_r="tb_clutch",
        q_s=1.8787467288586225e-06,
        q_r=1.3945872582756077e-06,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.005433004232599732,
        seed_s=0.0020071118460463847,
        seed_r=0.007205619086305153,
        tau2=0.0006672829299535788,
        mu_cells=(
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
            0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000,
        ),
    ),
    # -- match-box hold difficulty. Easy + difficult cover 5.1 of 11.2
    #    service games, so both are rates over the mb feed's OWN service-game
    #    count (mb_player_service_games == svc_games_played on only 81% of
    #    rows, so the svc count is the wrong denominator here). --
    _stream(
        "easyhold",
        ((1, "mb_player_easy_holds"),),
        ((1, "mb_player_service_games"),),
        seed_src_s="serve_elo",
        q_s=2.8329960261154117e-05,
        q_r=0.0,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.02492206926962594,
        seed_s=0.17466618550572802,
        seed_r=0.0,
        tau2=0.13752772725804352,
        mu_cells=(
            -0.3306376849, -0.2741193350, -0.4690888185, -0.3605678253,
            -0.5051155362, -0.4893638479, -0.6511755321, -0.5822464845,
            -0.1866640065, -0.4893638479, -0.2495545983, -0.4893638479,
        ),
    ),
    _stream(
        # k is not a favourable event, so no Elo seed (orientation rule).
        "hardhold",
        ((1, "mb_player_difficult_holds"),),
        ((1, "mb_player_service_games"),),
        q_s=1.0190643303020084e-06,
        q_r=0.0,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.005001898410918508,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.004086583220733103,
        mu_cells=(
            -2.1351049097, -2.1645787915, -2.1081979460, -2.1077411271,
            -2.0909609592, -2.1008336030, -2.0767006487, -2.0642163341,
            -2.0888333570, -2.1008336030, -2.1671922902, -2.1008336030,
        ),
    ),
    _stream(
        "deuce",
        ((1, "mb_player_games_multiple_deuces"),),
        ((1, "mb_player_service_games"),),
        q_s=1.0121461316782656e-06,
        q_r=0.0,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.011002553419409086,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.03152727172420931,
        mu_cells=(
            -2.1483245573, -2.2519300371, -2.0635281882, -2.1348786191,
            -2.0411936063, -2.0610479655, -1.9772624322, -1.9896203848,
            -2.2257083857, -2.0610479655, -2.2224698288, -2.0610479655,
        ),
    ),
    # -- point-pattern streams. `mb_` and `sp_` winners are DIFFERENT
    #    statistics (equal on 9% of the rows that carry both; means 21.7 vs
    #    16.8), so they are two streams and never coalesced. There is no
    #    sp_forced_errors column, hence the two-term sp denominator. --
    _stream(
        "net",
        ((1, "player_sp_net_points_won"),),
        ((1, "player_sp_net_points_played"),),
        has_returner=True,
        q_s=4.344387147524735e-06,
        q_r=2.6524702402162023e-06,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.0167644207577989,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.03440725187827185,
        mu_cells=(
            0.6163726836, 0.6542649151, 0.6093822960, 0.4948102084,
            0.5644451141, 0.6093822960, 0.5116253039, 0.6093822960,
            0.5780519172, 0.6093822960, 0.6093822960, 0.6093822960,
        ),
    ),
    _stream(
        "winner",
        ((1, "mb_player_winners"),),
        ((1, "mb_player_winners"), (1, "mb_player_ues"), (1, "mb_player_fes")),
        has_returner=True,
        q_s=0.0001411180629099492,
        q_r=0.0002824494277948153,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.02739620097687795,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.2331606076032959,
        mu_cells=(
            -0.7632939695, -0.7557413767, -0.6830765503, -0.5076381197,
            -0.3286772698, -0.6830765503, -0.1725982934, -0.6830765503,
            -0.6274760552, -0.6830765503, -0.6830765503, -0.6830765503,
        ),
    ),
    _stream(
        "sp_winner",
        ((1, "player_sp_winners"),),
        ((1, "player_sp_winners"), (1, "player_sp_unforced_errors")),
        has_returner=True,
        q_s=0.00026965544379633653,
        q_r=0.00013879829422801048,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.0550396914881449,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.17117478909663342,
        mu_cells=(
            0.3578236309, 0.4929415028, 0.4364692556, 0.6899056866,
            0.4801825170, 0.4364692556, 0.6197646346, 0.4364692556,
            0.6274336607, 0.4364692556, 0.4364692556, 0.4364692556,
        ),
    ),
    _rally(
        "rally_short", "short",
        q_s=0.0005965489897441902,
        q_r=0.0012761310865312373,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.12234695796441884,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.26972341644714176,
        mu_cells=(
            -0.1649702744, -0.0395541322, -0.0090313267, -0.0889474860,
            0.5337042711, -0.0090313267, 0.4802751796, -0.0090313267,
            0.1299261066, -0.0090313267, -0.0090313267, -0.0090313267,
        ),
    ),
    _rally(
        "rally_medium", "medium",
        q_s=0.00047408463583586085,
        q_r=0.0005653219023706663,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.044988644446157786,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.2930526845379799,
        mu_cells=(
            -0.2889760084, -0.1896438594, -0.1410151374, -0.0943936636,
            0.3504553242, -0.1410151374, 0.3674557821, -0.1410151374,
            0.0022684320, -0.1410151374, -0.1410151374, -0.1410151374,
        ),
    ),
    _rally(
        "rally_long", "long",
        q_s=0.0009151004521371354,
        q_r=0.0026832435608014405,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.15765430359526342,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.026544910565251066,
        mu_cells=(
            -0.4846864459, -0.4031949596, -0.3426805803, -0.3108963098,
            0.1899317378, -0.3426805803, 0.3353975455, -0.3426805803,
            -0.2798453650, -0.3426805803, -0.3426805803, -0.3426805803,
        ),
    ),
    _shot(
        "fh", "fh",
        q_s=0.00031063598133806856,
        q_r=0.00020131742453097782,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.04759468695664913,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.14353771450110078,
        mu_cells=(
            -0.2675459849, -0.2299815810, -0.2113366360, -0.0850586472,
            -0.0675164479, -0.2113366360, -0.0456455075, -0.2113366360,
            -0.0787539503, -0.2113366360, -0.2113366360, -0.2113366360,
        ),
    ),
    _shot(
        "bh", "bh",
        q_s=0.00029712177534863525,
        q_r=0.0002279068940468706,
        q_surf=0.0,
        q_indoor=0.0,
        v0=0.07713425966809148,
        seed_s=0.0,
        seed_r=0.0,
        tau2=0.17141082471208816,
        mu_cells=(
            -1.1859047559, -1.1454595729, -1.0842098736, -0.8161005099,
            -0.6819985691, -1.0842098736, -0.4120832660, -1.0842098736,
            -0.9882335055, -1.0842098736, -1.0842098736, -1.0842098736,
        ),
    ),
)

# One clock per feed (see StreamConfig.emit_counts): the pooled stream's
# count is shipped by name; these keep their own; every other stream's
# tracks one of them at r > 0.999. Residual spreads only on the pooled
# stream (the streams sharing a box score share them to four decimals).
_KEEP_COUNTS = {"serve", "bp", "tb", "easyhold", "winner", "sp_winner", "rally_short"}
STREAMS = tuple(
    _dc.replace(
        st,
        emit_counts=st.name in _KEEP_COUNTS,
        emit_residual_sd=(st.name == "serve"),
    )
    for st in STREAMS
)

STREAM_NAMES: tuple[str, ...] = tuple(s.name for s in STREAMS)
STREAM_INDEX: dict[str, int] = {s.name: i for i, s in enumerate(STREAMS)}
N_STREAMS = len(STREAMS)

# Per-set tiebreak score columns the two derived streams read.
TIEBREAK_COLUMNS: tuple[str, ...] = tuple(
    f"player_set{i}_tiebreak" for i in range(1, 6)
) + tuple(f"opp_set{i}_tiebreak" for i in range(1, 6))


def bsr_input_columns() -> list[str]:
    """Every aggregate column the streams read, both sides, deduplicated.

    The ratings pass builds its guard and the aggregator builds its slim
    frame from this, so a stream added to the table above cannot be silently
    fed all-null counts.
    """
    out: dict[str, None] = {}
    for st in STREAMS:
        for col in st.input_columns():
            out[col] = None
            out[mirror_col(col)] = None
    for col in TIEBREAK_COLUMNS:
        out[col] = None
    return list(out)


@dataclass(frozen=True)
class BsrConfig:
    """The filter's knobs. Field defaults are the tuned values.

    The scalar fields below are the SHIPPED `serve` stream's; they stay because
    the probe-parity test and `scripts/bsr/verify_ratings_pass.py` read them by
    name to rebuild the probe's parameter dict. Per-stream knobs live in
    `streams`; `cap_days`, `phi_surf` and `newton` are global and shared by
    every stream, as the plan's tuning protocol fixed them.
    """

    # Random-walk variance per day on the overall serve / return axes; the
    # elapsed-day count is capped so a long absence does not blow the prior.
    q_s: float = _Q_S
    q_r: float = _Q_R
    cap_days: float = _CAP_DAYS
    # Surface residual axes: AR(1) toward zero, applied once per observation on
    # that surface (the cadence measured as best), with this innovation.
    q_surf: float = _Q_SURF
    phi_surf: float = _PHI_SURF
    # Prior variance of a new player's overall axes.
    v0: float = _V0
    # Cold start: prior mean = seed * (pre-match serve/return Elo - 1500) / 100.
    seed_es: float = _SEED_ES
    seed_er: float = _SEED_ER
    # Per-observation random effect on eta (match-day form). Tuned per circuit
    # (0.0265 tour, 0.0369 chal); shipped tied to the tour value, which the
    # selection span measured as no loss.
    tau2: float = _TAU2
    newton: int = 2
    # Observations before this date are outside the tuning domain and are not
    # applied; state is emitted for them if it exists.
    start_date: date = date(2015, 1, 1)
    mu_cells: tuple[float, ...] = MU_CELLS
    streams: tuple[StreamConfig, ...] = STREAMS

    def stamp(self) -> dict[str, float]:
        return {
            "q_s": self.q_s, "q_r": self.q_r, "cap_days": self.cap_days,
            "q_surf": self.q_surf, "phi_surf": self.phi_surf, "v0": self.v0,
            "seed_es": self.seed_es, "seed_er": self.seed_er, "tau2": self.tau2,
            "newton": float(self.newton),
        }


DEFAULT_BSR_CONFIG = BsrConfig()
