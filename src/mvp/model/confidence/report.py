"""Text report formatting for confidence validation results."""

from __future__ import annotations

from mvp.model.confidence.metrics import ReliabilityProfile, WindowDistribution
from mvp.model.confidence.validator import ValidationResult

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
                          if k.startswith("consensus:")}
    identity_profiles = {k: v for k, v in result.profiles.items()
                         if k.startswith("consensus_id:")}

    if consensus_profiles:
        lines.append("")
        lines.append("--- CONSENSUS ---")
        for label in sorted(consensus_profiles, key=_consensus_sort_key):
            profiles = consensus_profiles[label]
            short_label = label.split(":")[1]
            overall = profiles.get("overall")
            if overall:
                lines.append("")
                lines.append(f"  {short_label} (n={overall.n_matches:,})")
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
    lines.append(
        f"{pad}cal: {sign}{p.signed_cal * 100:.1f}% ({direction}) | "
        f"acc: {p.accuracy * 100:.1f}% | err80: {p.err80 * 100:.1f}%"
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
