from pathlib import Path

from mercor_apex_finance_eval.types import TaskRecord
from mercor_apex_finance_eval.value_model import resolve_value_for_task


def test_resolve_value_for_task_uses_defaults_when_missing():
    task = TaskRecord(task_id=1, domain="Finance", prompt="Analyze this company.", rubric_json="{}")
    value = resolve_value_for_task(
        task,
        overrides={},
        default_hours=3.5,
        low_rate=100.0,
        base_rate=150.0,
        high_rate=250.0,
    )
    assert value.value_low_usd == 350.0
    assert value.value_base_usd == 525.0
    assert value.value_high_usd == 875.0
