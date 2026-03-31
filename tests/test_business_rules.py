from mercor_apex_finance_eval.business_rules import business_pass


def test_business_pass_requires_all_primary():
    passed, summary = business_pass(
        score_pct=100.0,
        criteria_results=[
            {"weight": "Primary objective(s)", "autorating": True},
            {"weight": "Primary objective(s)", "autorating": False},
            {"weight": "Not primary objective", "autorating": True},
        ],
        min_overall_score_pct=80.0,
        require_all_primary=True,
    )
    assert passed is False
    assert summary["primary_total"] == 2
    assert summary["primary_met"] == 1


def test_business_pass_allows_non_primary_failures():
    passed, summary = business_pass(
        score_pct=90.0,
        criteria_results=[
            {"weight": "Primary objective(s)", "autorating": True},
            {"weight": "Not primary objective", "autorating": False},
        ],
        min_overall_score_pct=80.0,
        require_all_primary=True,
    )
    assert passed is True
    assert summary["secondary_total"] == 1
