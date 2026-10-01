"""Config helpers shared by the bsr tests.

The default config carries the 2026-09-30 serve re-tune and newcomer terms.
Tests of the filter's mechanics run with the newcomer terms neutral; the
parity tests, which compare against filters that predate both, also pin the
`serve` stream to the literals shipped before the re-tune.
"""

from dataclasses import replace

from mvp.atptour.bsr.constants import BsrConfig

# The `serve` stream's q, v0, seed weights and tau2 as shipped before the
# 2026-09-30 joint re-tune (BsrConfig scalar field names).
SHIPPED_SERVE_SCALARS: dict[str, float] = {
    "q_s": 2.1859571733982455e-05,
    "q_r": 2.0316890516025758e-05,
    "v0": 0.062307747202284006,
    "seed_es": 0.3279483258118909,
    "seed_er": 0.2539311419400611,
    "tau2": 0.026544910565251066,
}


def neutral_newcomer(cfg: BsrConfig | None = None) -> BsrConfig:
    """`cfg` (default: the default config) with every stream's newcomer
    terms zero."""
    cfg = cfg or BsrConfig()
    return replace(cfg, streams=tuple(
        replace(st, nc_c_s=0.0, nc_c_r=0.0, nc_dr_s=0.0, nc_dr_r=0.0)
        for st in cfg.streams
    ))


def shipped_serve(cfg: BsrConfig, shipped: BsrConfig | None = None) -> BsrConfig:
    """`cfg` with the `serve` stream (index 0) and the config's scalar fields
    set to the pre-re-tune serve literals: `shipped`'s scalars when given (a
    config loaded from an older revision), else SHIPPED_SERVE_SCALARS."""
    if shipped is not None:
        sc = {k: getattr(shipped, k) for k in SHIPPED_SERVE_SCALARS}
    else:
        sc = dict(SHIPPED_SERVE_SCALARS)
    s0 = replace(
        cfg.streams[0], q_s=sc["q_s"], q_r=sc["q_r"], v0=sc["v0"],
        seed_s=sc["seed_es"], seed_r=sc["seed_er"], tau2=sc["tau2"],
    )
    return replace(cfg, streams=(s0,) + tuple(cfg.streams[1:]), **sc)
