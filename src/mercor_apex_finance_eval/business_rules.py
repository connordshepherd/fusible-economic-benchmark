from __future__ import annotations

from typing import Any


def is_primary_weight(weight: str | None) -> bool:
    normalized = (weight or "").strip().lower()
    return normalized in {"primary objective(s)", "primary objective", "primary"}


def criteria_summary(criteria_results: list[dict[str, Any]] | None) -> dict[str, int | bool]:
    criteria_results = criteria_results or []
    primary_total = 0
    primary_met = 0
    secondary_total = 0
    secondary_met = 0

    for criterion in criteria_results:
        autorating = bool(criterion.get("autorating"))
        if is_primary_weight(criterion.get("weight")):
            primary_total += 1
            if autorating:
                primary_met += 1
        else:
            secondary_total += 1
            if autorating:
                secondary_met += 1

    return {
        "primary_total": primary_total,
        "primary_met": primary_met,
        "secondary_total": secondary_total,
        "secondary_met": secondary_met,
        "all_primary_met": primary_met == primary_total if primary_total > 0 else True,
    }


def business_pass(
    *,
    score_pct: float,
    criteria_results: list[dict[str, Any]] | None,
    min_overall_score_pct: float,
    require_all_primary: bool,
) -> tuple[bool, dict[str, int | bool]]:
    summary = criteria_summary(criteria_results)
    if require_all_primary and not summary["all_primary_met"]:
        return False, summary
    if score_pct < min_overall_score_pct:
        return False, summary
    return True, summary
