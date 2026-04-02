from mercor_apex_finance_eval.daytona_backend import _sandbox_debug_snapshot, _state_text


class _State:
    def __str__(self) -> str:
        return "started"


class _FakeSandbox:
    id = "sandbox-123"
    name = "apex-task-python-test"
    state = _State()
    desired_state = _State()
    error_reason = None
    created_at = "2026-04-02T00:00:00Z"
    updated_at = "2026-04-02T00:01:00Z"
    runner_id = "runner-123"
    target = "us"
    cpu = 1
    memory = 2
    disk = 3
    snapshot = None
    toolbox_proxy_url = "https://proxy.example.test/toolbox"
    user = "daytona"
    network_block_all = True
    auto_stop_interval = 15
    labels = {"project": "economic-evals"}


def test_sandbox_debug_snapshot_normalizes_known_fields():
    snapshot = _sandbox_debug_snapshot(_FakeSandbox())

    assert snapshot["id"] == "sandbox-123"
    assert snapshot["name"] == "apex-task-python-test"
    assert snapshot["state"] == "started"
    assert snapshot["desired_state"] == "started"
    assert snapshot["runner_id"] == "runner-123"
    assert snapshot["labels"] == {"project": "economic-evals"}
    assert "error_reason" not in snapshot


class _EnumLikeState:
    def __init__(self, value: str) -> None:
        self.value = value

    def __str__(self) -> str:
        return "SandboxState.STARTED"


def test_state_text_prefers_underlying_value_over_str_representation():
    assert _state_text(_EnumLikeState("started")) == "started"
