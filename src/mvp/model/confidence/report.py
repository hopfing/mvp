"""Text report formatting for confidence validation results."""

from __future__ import annotations

from mvp.model.confidence.metrics import ReliabilityProfile
from mvp.model.confidence.validator import ValidationResult
from mvp.model.confidence.voter_analysis import (
    CoverageCurveResult,
    VoterCorrelationResult,
    VoterMarginalResult,
)

BUCKET_ORDER = [
    "50-55%", "55-60%", "60-65%", "65-70%", "70-75%",
    "75-80%", "80-85%", "85-90%", "90-95%", "95-100%",
]

ROUND_ORDER = ["Q1", "Q2", "Q3", "RR", "R128", "R64", "R32", "R16", "QF", "SF", "F"]


def format_report(result: ValidationResult, model_name: str = "") -> str:
    lines: list[str] = []
    sep = "=" * 70

    lines.append(sep)
    lines.append(f"  Confidence Validation: {model_name}")
    lines.append(f"  OOF Matches: {result.n_total:,}")
    lines.append(sep)

    if "overall" in result.profiles:
        lines.append("")
        lines.append("--- OVERALL ---")
        _format_slice_profiles(lines, result.profiles["overall"])

    mod_prefixes = _modifier_prefixes()
    structural = {k: v for k, v in result.profiles.items()
                  if ":" in k
                  and "/" not in k
                  and k.split(":")[0] not in mod_prefixes
                  and not k.startswith("consensus")
                  and k != "overall"}

    dim_groups: dict[str, list[tuple[str, dict[str, ReliabilityProfile]]]] = {}
    for label, profiles in sorted(structural.items()):
        dim = label.split(":")[0]
        dim_groups.setdefault(dim, []).append((label, profiles))

    for dim, entries in dim_groups.items():
        lines.append("")
        lines.append(f"--- STRUCTURAL: {dim.upper()} ---")
        for label, profiles in _sort_entries(entries, dim):
            short_label = label.split(":")[-1]
            overall = profiles.get("overall")
            if overall:
                lines.append("")
                lines.append(f"  {short_label} (n={overall.n_matches:,})")
                _format_profile_summary(lines, overall, indent=4)
                _format_bucket_breakdown(lines, profiles, indent=4)

    modifier_groups: dict[str, list[tuple[str, dict[str, ReliabilityProfile]]]] = {}
    for label, profiles in sorted(result.profiles.items()):
        if label == "overall":
            continue
        mod_name = label.split(":")[0]
        if mod_name in mod_prefixes:
            modifier_groups.setdefault(mod_name, []).append((label, profiles))

    for mod_name, entries in modifier_groups.items():
        lines.append("")
        lines.append(f"--- MODIFIER: {mod_name} ---")
        for label, profiles in entries:
            short_label = label.split(":")[-1]
            overall = profiles.get("overall")
            if overall:
                lines.append("")
                lines.append(f"  {short_label} (n={overall.n_matches:,})")
                _format_profile_summary(lines, overall, indent=4)
                _format_bucket_breakdown(lines, profiles, indent=4)

    # Consensus section (ensemble only)
    consensus_profiles = {k: v for k, v in result.profiles.items()
                          if k.startswith("consensus:") and "/" not in k}
    cross_profiles = {k: v for k, v in result.profiles.items()
                      if k.startswith("consensus:") and "/" in k}
    identity_profiles = {k: v for k, v in result.profiles.items()
                         if k.startswith("consensus_id:") and "/" not in k}

    if consensus_profiles:
        for cons_label in sorted(consensus_profiles, key=_consensus_sort_key):
            cons_short = cons_label.split(":")[1]
            cons_overall = consensus_profiles[cons_label].get("overall")
            if not cons_overall:
                continue

            lines.append("")
            lines.append(f"--- CONSENSUS: {cons_short} (n={cons_overall.n_matches:,}) ---")
            _format_profile_summary(lines, cons_overall, indent=2)
            _format_bucket_breakdown(lines, consensus_profiles[cons_label], indent=2)

            # Cross-cut structural slices for this consensus level
            prefix = f"{cons_label}/"
            sub = {k: v for k, v in cross_profiles.items() if k.startswith(prefix)}
            if sub:
                sub_groups: dict[str, list[tuple[str, dict[str, ReliabilityProfile]]]] = {}
                for label, profiles in sorted(sub.items()):
                    sub_label = label[len(prefix):]
                    dim = sub_label.split(":")[0]
                    sub_groups.setdefault(dim, []).append((sub_label, profiles))

                for dim, entries in sub_groups.items():
                    for sub_label, profiles in _sort_entries(entries, dim):
                        short = sub_label.split(":")[-1]
                        overall = profiles.get("overall")
                        if overall:
                            lines.append("")
                            lines.append(f"  {short} (n={overall.n_matches:,})")
                            _format_profile_summary(lines, overall, indent=4)
                            _format_bucket_breakdown(lines, profiles, indent=4)

    if identity_profiles:
        lines.append("")
        lines.append("--- CONSENSUS IDENTITY ---")
        for label in sorted(identity_profiles):
            profiles = identity_profiles[label]
            short_label = label.split(":")[1]
            overall = profiles.get("overall")
            if overall:
                lines.append("")
                lines.append(f"  {short_label} (n={overall.n_matches:,})")
                _format_profile_summary(lines, overall, indent=4)
                _format_bucket_breakdown(lines, profiles, indent=4)

    # Voter analysis sections
    if result.voter_correlation:
        lines.append("")
        _format_voter_correlation(lines, result.voter_correlation)

    if result.coverage_curve and result.coverage_curve.points:
        lines.append("")
        _format_coverage_curve(lines, result.coverage_curve)

    if result.voter_marginal and result.voter_marginal.voters:
        lines.append("")
        _format_voter_marginal(lines, result.voter_marginal)

    lines.append("")
    lines.append(sep)
    return "\n".join(lines)


def _modifier_prefixes() -> set[str]:
    from mvp.model.confidence.dimensions import MODIFIERS
    return {m.name for m in MODIFIERS}


def _consensus_sort_key(label: str) -> int:
    """Sort consensus labels by agreement count descending (3-0 before 2-1)."""
    parts = label.split(":")[1].split("-")
    return -int(parts[0])


def _sort_entries(entries, dim):
    if dim in ("round", "circuit+round"):
        order = {r: i for i, r in enumerate(ROUND_ORDER)}
        return sorted(entries, key=lambda e: order.get(e[0].split("+")[-1].split(":")[-1], 99))
    return entries


def _format_slice_profiles(lines, profiles):
    overall = profiles.get("overall")
    if overall:
        _format_profile_summary(lines, overall, indent=2)
        _format_bucket_breakdown(lines, profiles, indent=2)


def _format_profile_summary(lines, p, indent=2):
    pad = " " * indent
    direction = "underconfident" if p.signed_cal >= 0 else "overconfident"
    sign = "+" if p.signed_cal >= 0 else ""
    auc_str = f" | AUC: {p.roc_auc:.3f}" if p.roc_auc is not None else ""
    lines.append(
        f"{pad}cal: {sign}{p.signed_cal * 100:.1f}% ({direction}) | "
        f"acc: {p.accuracy * 100:.1f}% | err80: {p.err80 * 100:.1f}% | "
        f"LL: {p.log_loss:.4f} | BS: {p.brier_score:.4f}{auc_str}"
    )
    for label, dist in [("3mo", p.cal_3mo), ("6mo", p.cal_6mo), ("12mo", p.cal_12mo)]:
        if dist:
            _format_window_dist(lines, label, dist, indent)


def _format_window_dist(lines, label, d, indent):
    pad = " " * indent
    lines.append(
        f"{pad}  {label}: median {d.median * 100:+.1f}%, "
        f"IQR [{d.p25 * 100:+.1f}%, {d.p75 * 100:+.1f}%], "
        f"range [{d.min * 100:+.1f}%, {d.max * 100:+.1f}%], "
        f"{d.n_windows} windows (med n={d.median_n_per_window})"
    )


def _format_bucket_breakdown(lines, profiles, indent):
    pad = " " * indent
    buckets = [b for b in BUCKET_ORDER if b in profiles]
    if not buckets:
        return
    lines.append(f"{pad}  Prob buckets:")
    for bucket in buckets:
        p = profiles[bucket]
        sign = "+" if p.signed_cal >= 0 else ""
        iqr_3 = ""
        if p.cal_3mo:
            iqr_3 = f"3mo IQR [{p.cal_3mo.p25 * 100:+.1f}%,{p.cal_3mo.p75 * 100:+.1f}%]"
        iqr_6 = ""
        if p.cal_6mo:
            iqr_6 = f"6mo IQR [{p.cal_6mo.p25 * 100:+.1f}%,{p.cal_6mo.p75 * 100:+.1f}%]"
        lines.append(
            f"{pad}    {bucket:>8}  n={p.n_matches:<5}  "
            f"cal:{sign}{p.signed_cal * 100:.1f}%  {iqr_3}  {iqr_6}"
        )


def _format_voter_correlation(
    lines: list[str], corr: VoterCorrelationResult
) -> None:
    """Format voter correlation matrix."""
    lines.append("--- VOTER CORRELATION (binary pick agreement) ---")
    names = corr.voter_names
    # Column header
    col_w = 10
    header = " " * 12
    for name in names:
        header += f"{name:>{col_w}}"
    lines.append(header)

    for i, a in enumerate(names):
        row = f"  {a:<10}"
        for j, b in enumerate(names):
            if i == j:
                row += f"{'—':>{col_w}}"
            else:
                key = (a, b) if (a, b) in corr.pairs else (b, a)
                if key in corr.pairs:
                    row += f"{corr.pairs[key].agreement_pct:>{col_w - 1}.1f}%"
                else:
                    row += f"{'n/a':>{col_w}}"
        lines.append(row)

    # Disagreement detail
    lines.append("")
    lines.append("  When they disagree, who's right?")
    for (a, b), stats in sorted(corr.pairs.items()):
        if stats.n_disagree == 0:
            continue
        a_pct = f"{stats.disagree_a_correct_pct:.1f}%" if stats.disagree_a_correct_pct is not None else "n/a"
        b_pct = f"{stats.disagree_b_correct_pct:.1f}%" if stats.disagree_b_correct_pct is not None else "n/a"
        lines.append(
            f"    {a} vs {b}: {stats.n_disagree:,} disagreements — "
            f"{a} correct {a_pct}, {b} correct {b_pct}"
        )


def _format_coverage_curve(
    lines: list[str], curve: CoverageCurveResult
) -> None:
    """Format coverage vs quality curve."""
    lines.append("--- COVERAGE vs QUALITY CURVE ---")
    lines.append(
        f"  {'Threshold':>10}  {'Coverage':>8}  {'n':>7}  "
        f"{'Acc':>7}  {'Err80':>7}  {'Cal':>8}  {'LL':>8}"
    )
    for pt in curve.points:
        p = pt.profile
        sign = "+" if p.signed_cal >= 0 else ""
        lines.append(
            f"  {pt.threshold_pct:>9}%  {pt.coverage_pct:>7.1f}%  {pt.n_matches:>7,}  "
            f"{p.accuracy * 100:>6.1f}%  {p.err80 * 100:>6.1f}%  "
            f"{sign}{p.signed_cal * 100:>6.1f}%  {p.log_loss:>8.4f}"
        )


def _format_voter_marginal(
    lines: list[str], marginal: VoterMarginalResult
) -> None:
    """Format voter marginal value (leave-one-out)."""
    lines.append("--- VOTER MARGINAL VALUE (leave-one-out) ---")
    lines.append(
        f"  Baseline @100%: {marginal.baseline_cov_100:.1f}% coverage, "
        f"{marginal.baseline_acc_100 * 100:.1f}% acc"
    )
    lines.append(
        f"  Baseline @80%:  {marginal.baseline_cov_80:.1f}% coverage, "
        f"{marginal.baseline_acc_80 * 100:.1f}% acc"
    )
    lines.append("")
    lines.append(
        f"  {'Voter':<16} {'Scope%':>7}  "
        f"{'Cov@100':>8} {'Acc@100':>8} {'Cal@100':>8} {'E80@100':>8}  "
        f"{'Cov@80':>8} {'Acc@80':>8} {'Cal@80':>8} {'E80@80':>8}"
    )
    for v in marginal.voters:
        def _fmt_delta(val: float) -> str:
            sign = "+" if val >= 0 else ""
            return f"{sign}{val:.1f}%"

        def _fmt_delta_small(val: float) -> str:
            sign = "+" if val >= 0 else ""
            return f"{sign}{val * 100:.1f}%"

        lines.append(
            f"  -{v.name:<15} {v.scope_pct:>6.1f}%  "
            f"{_fmt_delta(v.cov_delta_100):>8} {_fmt_delta_small(v.acc_delta_100):>8} "
            f"{_fmt_delta_small(v.cal_delta_100):>8} {_fmt_delta_small(v.err80_delta_100):>8}  "
            f"{_fmt_delta(v.cov_delta_80):>8} {_fmt_delta_small(v.acc_delta_80):>8} "
            f"{_fmt_delta_small(v.cal_delta_80):>8} {_fmt_delta_small(v.err80_delta_80):>8}"
        )
