"""Rule-based alerting on live predictions.

Rules live in ``alerts.yaml`` at the repo root. A rule is a set of conditions
that must all hold for a prediction to trigger it; the resulting Discord post
carries per-rule counts rather than the matching fixtures, so the alert says
"go look at the sheet" rather than trying to reproduce it.

Rules are evaluated every run against every open prediction, not only ones
appearing for the first time. A match whose price drifts into qualifying range
hours after it first appeared still alerts. A JSONL ledger keyed on
(rule, match_uid) is what stops that same match re-firing every 15 minutes.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import yaml

from mvp.gsheets.base import CIRCUIT_LABELS_INVERSE

logger = logging.getLogger(__name__)

# The sheet's date/time columns are already converted to venue-agnostic CT by
# prepare_predictions, so start times are compared in CT.
CT = ZoneInfo("America/Chicago")

# Fields a rule may match on. Numeric fields take a threshold; string fields
# compare case-insensitively. Anything outside these sets is a typo, and a
# typo'd condition would leave a rule silently never firing — so load rejects
# it rather than letting it through.
NUMERIC_FIELDS = frozenset({
    "edge", "edge_open", "pred_prob", "pred_odds",
    "consensus", "cell_cal", "age_diff",
})
STRING_FIELDS = frozenset({
    "circuit", "surface", "court", "round", "tournament",
    "cal_tier", "book", "prediction",
})
RULE_FIELDS = NUMERIC_FIELDS | STRING_FIELDS


@dataclass(frozen=True)
class Rule:
    """One named set of conditions, all of which must hold."""

    name: str
    conditions: dict[str, object]


def _is_number(value: object) -> bool:
    # bool is an int subclass; no field takes one, and letting `True` through
    # as 1 would silently accept a nonsense condition.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_condition(path: Path, rule: str, field: str, spec: object) -> None:
    if field not in RULE_FIELDS:
        raise ValueError(
            f"{path}: rule {rule!r} matches on unknown field {field!r}. "
            f"Known fields: {', '.join(sorted(RULE_FIELDS))}"
        )
    if field in NUMERIC_FIELDS:
        if _is_number(spec):
            return
        if isinstance(spec, dict):
            unknown = set(spec) - {"min", "max"}
            if not spec or unknown:
                raise ValueError(
                    f"{path}: rule {rule!r} field {field!r} takes 'min' and/or "
                    f"'max'" + (f", got {sorted(unknown)}" if unknown else "")
                )
            for key, value in spec.items():
                if not _is_number(value):
                    raise ValueError(
                        f"{path}: rule {rule!r} field {field!r} '{key}' "
                        f"must be a number"
                    )
            return
        raise ValueError(
            f"{path}: rule {rule!r} field {field!r} takes a number (an "
            f"inclusive minimum) or a mapping with 'min' and/or 'max'"
        )
    if isinstance(spec, str):
        return
    if isinstance(spec, list) and spec and all(isinstance(v, str) for v in spec):
        return
    raise ValueError(
        f"{path}: rule {rule!r} field {field!r} takes a string or a "
        f"non-empty list of strings"
    )


def load_rules(path: Path | str = "alerts.yaml") -> list[Rule]:
    """Parse and validate the rules file.

    A missing file means alerting is simply off, so the live pipeline runs
    unchanged on a host that has never configured any rules. A malformed file
    raises: a rule that can't be parsed is a rule the user believes is
    watching for them.
    """
    path = Path(path)
    if not path.exists():
        return []

    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top level must be a mapping")

    entries = raw.get("rules") or []
    if not isinstance(entries, list):
        raise ValueError(f"{path}: 'rules' must be a list")

    rules: list[Rule] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: rule at position {i} must be a mapping")

        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"{path}: rule at position {i} needs a non-empty 'name'"
            )
        name = name.strip()
        if name in seen:
            # The ledger keys on the name, so duplicates would share dedupe
            # state and one would mask the other.
            raise ValueError(f"{path}: duplicate rule name {name!r}")
        seen.add(name)

        conditions = entry.get("match")
        if not isinstance(conditions, dict) or not conditions:
            raise ValueError(f"{path}: rule {name!r} needs a non-empty 'match'")
        for field, spec in conditions.items():
            _validate_condition(path, name, field, spec)

        rules.append(Rule(name=name, conditions=dict(conditions)))

    return rules


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _as_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def row_fields(row: dict) -> dict[str, float | str | None]:
    """Project a merged-sheet row onto the fields rules match on.

    Most columns come back from the sheet round-trip as strings, so everything
    is coerced here. Anything missing or unparseable becomes None, which no
    condition accepts.
    """
    prediction = _as_text(row.get("prediction"))
    pred_prob = _as_float(row.get("pred_prob"))

    pred_odds = None
    if prediction == "P1":
        pred_odds = _as_float(row.get("p1_odds"))
    elif prediction == "P2":
        pred_odds = _as_float(row.get("p2_odds"))

    # Mirrors the sheet's fav_edge formula: the pick's probability against its
    # best current price across books. The sheet holds that as a formula, so
    # there is no numeric edge on the frame to read.
    edge = None
    if pred_prob is not None and pred_odds is not None and pred_odds > 0:
        edge = pred_prob - (1.0 / pred_odds)

    circuit = _as_text(row.get("circuit"))
    if circuit is not None:
        # The sheet carries display labels (ATP/CH); rules speak the raw
        # tour/chal vocabulary that the models and backtests use.
        circuit = CIRCUIT_LABELS_INVERSE.get(circuit, circuit)

    return {
        "edge": edge,
        "edge_open": _as_float(row.get("fav_edge_open")),
        "pred_prob": pred_prob,
        "pred_odds": pred_odds,
        "consensus": _as_float(row.get("consensus")),
        "cell_cal": _as_float(row.get("cell_cal")),
        "age_diff": _as_float(row.get("age_diff")),
        "circuit": circuit,
        "surface": _as_text(row.get("surface")),
        "court": _as_text(row.get("court")),
        "round": _as_text(row.get("round")),
        "tournament": _as_text(row.get("tournament")),
        "cal_tier": _as_text(row.get("cal_tier")),
        "book": _as_text(row.get("book")),
        "prediction": prediction,
    }


def _condition_holds(spec: object, value: float | str | None) -> bool:
    if value is None:
        return False
    if _is_number(spec):
        # A bare number is an inclusive minimum.
        return value >= spec
    if isinstance(spec, dict):
        low, high = spec.get("min"), spec.get("max")
        if low is not None and value < low:
            return False
        if high is not None and value > high:
            return False
        return True
    text = str(value).casefold()
    if isinstance(spec, str):
        return text == spec.casefold()
    return any(text == option.casefold() for option in spec)


def rule_matches(rule: Rule, fields: dict[str, float | str | None]) -> bool:
    """True when every condition on the rule holds for this prediction."""
    return all(
        _condition_holds(spec, fields.get(field))
        for field, spec in rule.conditions.items()
    )


def is_open(row: dict, now: datetime) -> bool:
    """True for a prediction still worth alerting on.

    A bet already placed (a stake is entered) needs no alert, and neither does
    a match that has started.
    """
    if _as_text(row.get("stake")):
        return False

    date = _as_text(row.get("date"))
    if date is None:
        return False

    time = _as_text(row.get("time"))
    if time:
        try:
            start = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
        except ValueError:
            return True
        return start.replace(tzinfo=CT) > now

    # No scheduled time on the row. Fall back to the date so a match with an
    # unknown start still alerts on its own day rather than being skipped.
    try:
        day = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return True
    return day >= now.date()


def load_ledger(path: Path | str) -> set[tuple[str, str]]:
    """Read the (rule, match_uid) pairs that have already fired.

    A malformed line is skipped rather than failing the run; the cost is one
    duplicate alert, which beats taking down the pipeline stage.
    """
    path = Path(path)
    if not path.exists():
        return set()

    fired: set[tuple[str, str]] = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed line in alert ledger %s", path)
                continue
            rule, uid = entry.get("rule"), entry.get("match_uid")
            if rule and uid:
                fired.add((rule, uid))
    return fired


def append_ledger(
    path: Path | str, fires: list[tuple[str, str]], now: datetime
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = now.isoformat()
    with open(path, "a") as f:
        for rule, uid in fires:
            f.write(
                json.dumps({"rule": rule, "match_uid": uid, "fired_at": stamp})
                + "\n"
            )


def find_new_fires(
    merged: pl.DataFrame,
    rules: list[Rule],
    fired: set[tuple[str, str]],
    now: datetime,
) -> list[tuple[str, str]]:
    """Return the (rule, match_uid) pairs triggered for the first time."""
    new: list[tuple[str, str]] = []
    if not rules or len(merged) == 0:
        return new

    seen = set(fired)
    for row in merged.iter_rows(named=True):
        uid = _as_text(row.get("match_uid"))
        if uid is None or not is_open(row, now):
            continue
        fields = row_fields(row)
        for rule in rules:
            key = (rule.name, uid)
            if key in seen:
                continue
            if rule_matches(rule, fields):
                seen.add(key)
                new.append(key)
    return new


def run(
    merged: pl.DataFrame,
    *,
    ledger_path: Path | str,
    rules_path: Path | str = "alerts.yaml",
    now: datetime | None = None,
) -> dict[str, int]:
    """Evaluate the rules against the merged sheet frame and record what fired.

    Returns rule name -> count of predictions triggering it for the first
    time. The ledger is written before the caller posts, so a webhook outage
    costs one dropped alert rather than re-alerting the same match every run.
    """
    now = now or datetime.now(CT)
    rules = load_rules(rules_path)
    if not rules:
        return {}

    fires = find_new_fires(merged, rules, load_ledger(ledger_path), now)
    if not fires:
        return {}
    append_ledger(ledger_path, fires, now)

    counts: dict[str, int] = {}
    for rule_name, _ in fires:
        counts[rule_name] = counts.get(rule_name, 0) + 1
    return counts
