from mercor_apex_finance_eval.config import GraderSettings, ModelSettings
from mercor_apex_finance_eval.mercor_adapter import _build_openai_request as build_plain_request
from mercor_apex_finance_eval.tool_agent import _build_openai_request as build_tool_request


def test_gpt5_requests_omit_temperature():
    grader = GraderSettings(model_id="gpt-5.4", temperature=0.01, max_tokens=1024)
    request = build_plain_request(grader)
    assert "temperature" not in request

    model = ModelSettings(model_id="gpt-5.4", temperature=0.7, max_tokens=1024)
    tool_request = build_tool_request(model, instructions="Test", tools=[])
    assert "temperature" not in tool_request


def test_gpt4_requests_keep_temperature():
    grader = GraderSettings(model_id="gpt-4o", temperature=0.01, max_tokens=1024)
    request = build_plain_request(grader)
    assert request["temperature"] == 0.01

    model = ModelSettings(model_id="gpt-4o", temperature=0.7, max_tokens=1024)
    tool_request = build_tool_request(model, instructions="Test", tools=[])
    assert tool_request["temperature"] == 0.7
