"""Decision-rule evaluation for rules_* configs.

A rules config is NOT scored like a probability model. It is a hand-authored
decision rule: each flag votes +1 / -1 / 0, and a player is *picked* as the
winner when more flags favor them than the opponent (net > 0). Ties and
no-flag matches abstain (no pick).

Because the data carries both player perspectives, a pick is exactly a row
with net > 0 — the mirror row (net < 0) is automatically not a pick, so there
is no double counting. Coverage is measured against distinct matches.

The report cross-tabs accuracy by vote configuration (3-0, 2-0, 2-1, 1-0)
against round, surface, and surface-elo-diff band, nested under circuit, over
the full date range (the rule is fixed — there is no train/test split).

Run via the CLI:  poetry run py -m mvp rules rules_combined
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from mvp.model.config import ExperimentConfig, apply_filters, get_filter_feature_specs
from mvp.model.engine import get_feature_columns
from mvp.model.rule_model import flag_vote_matrix, resolve_flags
from mvp.model.runner import ExperimentRunner

# Columns needed beyond the flag features: outcome, segment splits, identity.
_EXTRA_COLS = ["won", "circuit", "surface", "round", "match_uid", "player_id", "opp_id"]
# Surface-elo diff (picked player - opp), computed for the rating breakdown only.
_RATING_COL = "player_elo_surface_diff"


def compute_votes(config_path: Path) -> tuple[ExperimentConfig, list[dict], pl.DataFrame]:
    """Compute per-row votes for a rules config.

    Returns (config, resolved_flags, df) where df has the flag features plus
    for_count / against_count / net / year and the segment/outcome columns.
    """
    config = ExperimentConfig.from_file(str(config_path))
    if config.model.type != "rules":
        raise ValueError(
            f"{config_path} is model.type={config.model.type!r}, not 'rules'"
        )
    engine = ExperimentRunner(config_path=config_path).engine

    specs = list(config.features.include)
    # Parameterized filter-features that aren't model features still need computing;
    # _RATING_COL is computed for the rating breakdown (it is not a flag).
    filter_specs = get_filter_feature_specs(config.data.filters)
    extra_specs = filter_specs + [_RATING_COL]
    all_specs = specs + [s for s in extra_specs if s not in specs]
    computed_cols = set(get_feature_columns(all_specs))
    # Raw filter columns (e.g. draw_type) aren't computed features — project them
    # too, mirroring how the runner adds every filter key to its column list.
    extra = list(_EXTRA_COLS)
    for col in (config.data.filters or {}):
        if col not in extra and col not in computed_cols:
            extra.append(col)
    df = engine.compute(all_specs, extra_columns=extra)

    dr = config.data.date_range
    df = df.filter(
        (pl.col("effective_match_date") >= dr.start)
        & (pl.col("effective_match_date") <= dr.end)
        & pl.col("won").is_not_null()
    )
    if config.data.filters:
        df = apply_filters(df, config.data.filters)

    feature_cols = get_feature_columns(specs)
    resolved = resolve_flags(config.model.params["flags"], feature_cols)
    X = df.select(feature_cols).to_numpy()
    votes = flag_vote_matrix(X, resolved)
    for_count = (votes == 1).sum(axis=1)
    against_count = (votes == -1).sum(axis=1)

    df = df.with_columns(
        pl.Series("for_count", for_count, dtype=pl.Int64),
        pl.Series("against_count", against_count, dtype=pl.Int64),
        pl.Series("net", (for_count - against_count), dtype=pl.Int64),
        pl.col("effective_match_date").dt.year().alias("year"),
    ).with_columns(
        (pl.col("for_count").cast(pl.Utf8) + "-" + pl.col("against_count").cast(pl.Utf8))
        .alias("config"),
    )
    return config, resolved, df


def summarize(df: pl.DataFrame) -> dict:
    """Headline numbers from a votes dataframe (net / for_count / against_count).

    Predictions are net>0 rows (one per two-perspective match). Returns counts,
    coverage, accuracy, the by-configuration table, and a single-perspective
    abstain count. Pure (no I/O) so it is unit-testable without the engine.
    """
    unique_matches = df["match_uid"].n_unique()
    # One pick per match: the perspective with the highest net vote, then net>0.
    # (Symmetric gating already yields one net>0 row; per-side gating can make
    # both rows net>0, so take the stronger side rather than double-count.)
    best = df.sort("net", descending=True).unique(subset="match_uid", keep="first")
    picks = best.filter(pl.col("net") > 0)
    n_picks = picks.height
    # A lone (single-perspective) row with net<=0 can't carry a pick.
    pm = df.group_by("match_uid").agg(
        pl.len().alias("n_rows"), pl.col("net").max().alias("max_net")
    )
    single_no_pick = pm.filter(
        (pl.col("n_rows") == 1) & (pl.col("max_net") <= 0)
    ).height
    by_config = (
        picks.with_columns(
            (pl.col("for_count").cast(pl.Utf8) + "-"
             + pl.col("against_count").cast(pl.Utf8)).alias("config")
        )
        .group_by("config", "for_count", "against_count")
        .agg(pl.len().alias("picks"), pl.col("won").mean().alias("acc"))
        .sort("for_count", "against_count", descending=[True, False])
    )
    return {
        "unique_matches": unique_matches,
        "n_picks": n_picks,
        "coverage": n_picks / unique_matches if unique_matches else float("nan"),
        "accuracy": picks["won"].mean() if n_picks else float("nan"),
        "single_no_pick": single_no_pick,
        "by_config": by_config,
        "picks": picks,
    }


def _config_order(picks: pl.DataFrame) -> list[str]:
    """Configs sorted by net (desc) then for-count (desc): 3-0, 2-0, 2-1, 1-0."""
    pairs = picks.select("for_count", "against_count").unique()
    items = sorted(
        ((r["for_count"], r["against_count"]) for r in pairs.iter_rows(named=True)),
        key=lambda fa: (-(fa[0] - fa[1]), -fa[0]),
    )
    return [f"{f}-{a}" for f, a in items]


def _crosstab_cells(picks: pl.DataFrame, row_col: str) -> tuple[dict, dict]:
    """(row, config) -> (n, acc) and row -> (n, acc). Pure; unit-testable."""
    g = picks.group_by(row_col, "config").agg(
        pl.len().alias("n"), pl.col("won").mean().alias("acc")
    )
    cells = {(r[row_col], r["config"]): (r["n"], r["acc"]) for r in g.iter_rows(named=True)}
    ag = picks.group_by(row_col).agg(
        pl.len().alias("n"), pl.col("won").mean().alias("acc")
    )
    allc = {r[row_col]: (r["n"], r["acc"]) for r in ag.iter_rows(named=True)}
    return cells, allc


def _fmt(cell: tuple | None) -> str:
    if not cell or cell[0] in (None, 0):
        return "-"
    n, acc = cell
    return f"{acc * 100:.1f}%({n:,})"


def _print_crosstab(title: str, picks: pl.DataFrame, row_col: str,
                    configs: list[str], row_order: list) -> None:
    cells, allc = _crosstab_cells(picks, row_col)
    w = 15
    print(f"\n  {title}")
    print("    " + f"{'':<12}" + "".join(f"{c:>{w}}" for c in configs) + f"{'all':>{w}}")
    for rv in row_order:
        if rv not in allc:
            continue
        body = "".join(_fmt(cells.get((rv, c))).rjust(w) for c in configs)
        print(f"    {str(rv):<12}{body}{_fmt(allc[rv]).rjust(w)}")


def _round_order(picks: pl.DataFrame) -> list:
    """Rounds in chronological order via round_order (fallback: by pick count)."""
    if "round_order" not in picks.columns:
        return (picks.group_by("round").agg(pl.len().alias("n"))
                .sort("n", descending=True)["round"].to_list())
    return (picks.group_by("round").agg(pl.col("round_order").min().alias("ro"))
            .sort("ro")["round"].to_list())


def _surface_order(picks: pl.DataFrame) -> list:
    return (picks.group_by("surface").agg(pl.len().alias("n"))
            .sort("n", descending=True)["surface"].to_list())


def _add_elo_bands(picks: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    """Quintile bands of the picked player's surface-elo edge over the opponent."""
    ev = picks.filter(pl.col(_RATING_COL).is_not_null())
    if ev.height == 0:
        return picks.with_columns(pl.lit(None).alias("elo_band")), []
    raw = [ev[_RATING_COL].quantile(q) for q in (0.2, 0.4, 0.6, 0.8)]
    breaks = sorted({int(round(x)) for x in raw})
    labels = (
        [f"<={breaks[0]}"]
        + [f"{breaks[i-1]}..{breaks[i]}" for i in range(1, len(breaks))]
        + [f">{breaks[-1]}"]
    )
    picks = picks.with_columns(
        pl.col(_RATING_COL).cut(breaks, labels=labels).alias("elo_band")
    )
    return picks, labels


def print_report(name: str, config: ExperimentConfig, resolved: list[dict],
                 df: pl.DataFrame) -> None:
    s = summarize(df)
    picks, elo_labels = _add_elo_bands(s["picks"])
    configs = _config_order(picks)
    round_order = _round_order(picks)
    surface_order = _surface_order(picks)
    year_order = sorted(picks["year"].unique().to_list())
    bar = "=" * 78

    print(bar)
    print(f"  RULES EVAL: {name}")
    print(bar)
    for r in resolved:
        bits = [r["label"], f"pivot {r['pivot']:g}", f"db {r['deadband']:g}"]
        if r["min_matches"] is not None:
            bits.append(f"min {r['min_matches']}")
        print("  " + " | ".join(bits))
    dr = config.data.date_range
    print(f"  {dr.start}..{dr.end} | {s['unique_matches']:,} matches | "
          f"{s['n_picks']:,} picks ({s['coverage']*100:.1f}%) | "
          f"{s['accuracy']*100:.2f}% acc")

    print("\nCONFIG")
    print(f"  {'cfg':<6}{'picks':>11}{'cov':>8}{'acc':>8}")
    for r in s["by_config"].iter_rows(named=True):
        cov = r["picks"] / s["unique_matches"] if s["unique_matches"] else 0.0
        print(f"  {r['config']:<6}{r['picks']:>11,}{cov*100:7.1f}%{r['acc']*100:7.1f}%")

    circuit_order = (picks.group_by("circuit").agg(pl.len().alias("n"))
                     .sort("n", descending=True)["circuit"].to_list())
    for circ in circuit_order:
        cp = picks.filter(pl.col("circuit") == circ)
        cmatches = df.filter(pl.col("circuit") == circ)["match_uid"].n_unique()
        cn = cp.height
        print(f"\n{bar}")
        print(f"  {str(circ).upper()}  |  {cmatches:,} matches | {cn:,} picks "
              f"({cn/cmatches*100:.1f}%) | {cp['won'].mean()*100:.2f}% acc")
        print(bar)
        _print_crosstab("round x config", cp, "round", configs, round_order)
        _print_crosstab("surface x config", cp, "surface", configs, surface_order)
        _print_crosstab("year x config", cp, "year", configs, year_order)
        if elo_labels:
            _print_crosstab(f"{_RATING_COL} (picked - opp) x config",
                            cp, "elo_band", configs, elo_labels)
