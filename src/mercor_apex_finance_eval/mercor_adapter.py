from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from openai import OpenAI

from .config import GraderSettings, ModelSettings, PricingSettings
from .pricing import openai_cost_usd, openai_price_book_id
from .tool_agent import _response_text, _usage_totals
from .utils import jsonable, utc_now_iso


def _is_openai_model(model_id: str) -> bool:
    lowered = model_id.lower()
    return lowered.startswith("gpt-") or lowered.startswith("o")


def _supports_temperature(model_id: str) -> bool:
    lowered = model_id.lower()
    if lowered.startswith("gpt-5") or lowered.startswith("o"):
        return False
    return True


def _build_openai_request(
    model: ModelSettings | GraderSettings,
    *,
    instructions: str | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "model": model.model_id,
        "max_output_tokens": getattr(model, "max_tokens", None) or 65535,
    }
    if instructions:
        request["instructions"] = instructions

    temperature = getattr(model, "temperature", None)
    if temperature is not None and _supports_temperature(model.model_id):
        request["temperature"] = temperature

    model_configs = getattr(model, "model_configs", {}) or {}
    reasoning_effort = model_configs.get("reasoning_effort")
    if reasoning_effort:
        request["reasoning"] = {"effort": reasoning_effort}

    verbosity = model_configs.get("verbosity")
    if verbosity:
        request["text"] = {"verbosity": verbosity}

    return request


def _failure_generation(message: str) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "success": False,
        "response": "",
        "raw_response": "",
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "total_cost": 0.0,
        "api_provider": "openai",
        "execution_time_seconds": None,
        "error_message": message,
        "started_at": now,
        "completed_at": now,
        "details": {"error": message},
    }


def _failure_grading(message: str, *, points_possible: int) -> dict[str, Any]:
    return {
        "points_earned": 0.0,
        "points_possible": points_possible,
        "percentage_score": 0.0,
        "criteria_results": [],
        "grading_error": message,
        "execution_time_seconds": None,
        "total_grading_input_tokens": 0,
        "total_grading_cached_input_tokens": 0,
        "total_grading_output_tokens": 0,
        "total_grading_tokens": 0,
        "total_grading_cost": 0.0,
        "price_book_id": "",
    }


def _extract_json_object(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        raise ValueError("Model returned an empty grading response.")

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        start = payload.find("{")
        end = payload.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(payload[start : end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("Expected a JSON object from the grader.")
    return parsed


def _criterion_prompt_context(key: str, criterion: dict[str, Any]) -> str:
    parts = [f"{key}: {criterion.get('description', '')}".strip()]
    sources = criterion.get("sources")
    if sources:
        parts.append(f"Sources: {sources}")
    justification = criterion.get("justification")
    if justification:
        parts.append(f"Rubric intent: {justification}")
    return "\n".join(parts)


def _normalize_rubric(rubric_json: str) -> list[tuple[str, dict[str, Any]]]:
    rubric = json.loads(rubric_json)
    if isinstance(rubric, dict):
        return [(str(key), value if isinstance(value, dict) else {"description": str(value)}) for key, value in rubric.items()]
    if isinstance(rubric, list):
        return [
            (f"criterion {index}", value if isinstance(value, dict) else {"description": str(value)})
            for index, value in enumerate(rubric, start=1)
        ]
    raise ValueError("Rubric JSON must decode to a dict or list.")


def _run_generation_once_sync(
    *,
    prompt: str,
    system_prompt: str | None,
    model: ModelSettings,
    pricing: PricingSettings,
) -> dict[str, Any]:
    if not _is_openai_model(model.model_id):
        return _failure_generation(
            f"LiteLLM has been removed. Plain generation currently supports only direct OpenAI models, not `{model.model_id}`."
        )

    client = OpenAI(timeout=120.0, max_retries=2)
    started_at = utc_now_iso()
    started = time.perf_counter()
    response: Any = None
    try:
        response = client.responses.create(
            input=prompt,
            **_build_openai_request(model, instructions=system_prompt),
        )
        input_tokens, output_tokens, total_tokens, cached_tokens = _usage_totals(response)
        completed_at = utc_now_iso()
        return {
            "success": True,
            "response": _response_text(response),
            "raw_response": json.dumps(jsonable(response), ensure_ascii=False),
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "total_cost": openai_cost_usd(
                pricing.openai_price_book,
                model_id=model.model_id,
                input_tokens=input_tokens,
                cached_input_tokens=cached_tokens,
                output_tokens=output_tokens,
            ) or 0.0,
            "api_provider": "openai",
            "execution_time_seconds": time.perf_counter() - started,
            "error_message": "",
            "started_at": started_at,
            "completed_at": completed_at,
            "details": {
                "price_book_id": openai_price_book_id(pricing.openai_price_book),
                "response": jsonable(response),
            },
        }
    except Exception as exc:
        completed_at = utc_now_iso()
        return {
            "success": False,
            "response": "",
            "raw_response": json.dumps(jsonable(response), ensure_ascii=False) if response is not None else "",
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "total_cost": 0.0,
            "api_provider": "openai",
            "execution_time_seconds": time.perf_counter() - started,
            "error_message": str(exc),
            "started_at": started_at,
            "completed_at": completed_at,
            "details": {"exception_type": type(exc).__name__},
        }


async def run_generation_once(
    *,
    prompt: str,
    system_prompt: str | None,
    model: ModelSettings,
    pricing: PricingSettings,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_generation_once_sync,
        prompt=prompt,
        system_prompt=system_prompt,
        model=model,
        pricing=pricing,
    )


def _run_grading_once_sync(
    *,
    solution: str,
    rubric_json: str,
    grader: GraderSettings,
    grading_prompt_template: str,
    pricing: PricingSettings,
) -> dict[str, Any]:
    criteria = _normalize_rubric(rubric_json)
    if not _is_openai_model(grader.model_id):
        return _failure_grading(
            f"LiteLLM has been removed. Grading currently supports only direct OpenAI models, not `{grader.model_id}`.",
            points_possible=len(criteria),
        )

    client = OpenAI(timeout=120.0, max_retries=2)
    started = time.perf_counter()
    total_input_tokens = 0
    total_cached_input_tokens = 0
    total_output_tokens = 0
    total_tokens = 0
    total_cost = 0.0
    points_earned = 0.0
    criteria_results: list[dict[str, Any]] = []
    errors: list[str] = []

    for criterion_index, (criterion_key, criterion) in enumerate(criteria, start=1):
        prompt = grading_prompt_template.format(
            criterion_description=_criterion_prompt_context(criterion_key, criterion),
            solution=solution,
        )
        response: Any = None
        criterion_started = time.perf_counter()
        try:
            response = client.responses.create(
                input=prompt,
                **_build_openai_request(grader),
            )
            input_tokens, output_tokens, criterion_tokens, cached_tokens = _usage_totals(response)
            total_input_tokens += input_tokens
            total_cached_input_tokens += cached_tokens
            total_output_tokens += output_tokens
            total_tokens += criterion_tokens
            criterion_cost = openai_cost_usd(
                pricing.openai_price_book,
                model_id=grader.model_id,
                input_tokens=input_tokens,
                cached_input_tokens=cached_tokens,
                output_tokens=output_tokens,
            ) or 0.0
            total_cost += criterion_cost

            raw_response = _response_text(response)
            parsed = _extract_json_object(raw_response)
            autorating = bool(int(parsed.get("result", 0)))
            if autorating:
                points_earned += 1.0

            criteria_results.append(
                {
                    "criterion_key": criterion_key,
                    "description": criterion.get("description", ""),
                    "weight": criterion.get("weight"),
                    "sources": criterion.get("sources"),
                    "criterion_type": criterion.get("criterion_type", []),
                    "dependent_criteria": criterion.get("dependent_criteria", []),
                    "autorating": autorating,
                    "reason": str(parsed.get("reason", "")).strip(),
                    "criterion_index": criterion_index,
                    "grading_success": True,
                    "grading_error": None,
                    "tokens_used": criterion_tokens,
                    "input_tokens": input_tokens,
                    "cached_input_tokens": cached_tokens,
                    "output_tokens": output_tokens,
                    "total_cost": criterion_cost,
                    "execution_time_seconds": time.perf_counter() - criterion_started,
                    "raw_response": raw_response,
                }
            )
        except Exception as exc:
            errors.append(f"{criterion_key}: {exc}")
            criteria_results.append(
                {
                    "criterion_key": criterion_key,
                    "description": criterion.get("description", ""),
                    "weight": criterion.get("weight"),
                    "sources": criterion.get("sources"),
                    "criterion_type": criterion.get("criterion_type", []),
                    "dependent_criteria": criterion.get("dependent_criteria", []),
                    "autorating": False,
                    "reason": f"Grading failed: {exc}",
                    "criterion_index": criterion_index,
                    "grading_success": False,
                    "grading_error": str(exc),
                    "tokens_used": 0,
                    "input_tokens": 0,
                    "cached_input_tokens": 0,
                    "output_tokens": 0,
                    "total_cost": 0.0,
                    "execution_time_seconds": time.perf_counter() - criterion_started,
                    "raw_response": _response_text(response) if response is not None else "",
                }
            )

    points_possible = len(criteria)
    percentage_score = (points_earned / points_possible * 100.0) if points_possible else 0.0

    return {
        "points_earned": points_earned,
        "points_possible": points_possible,
        "percentage_score": percentage_score,
        "criteria_results": criteria_results,
        "grading_error": "; ".join(errors) if errors else None,
        "execution_time_seconds": time.perf_counter() - started,
        "total_grading_input_tokens": total_input_tokens,
        "total_grading_cached_input_tokens": total_cached_input_tokens,
        "total_grading_output_tokens": total_output_tokens,
        "total_grading_tokens": total_tokens,
        "total_grading_cost": total_cost,
        "price_book_id": openai_price_book_id(pricing.openai_price_book),
    }


async def run_grading_once(
    *,
    solution: str,
    rubric_json: str,
    grader: GraderSettings,
    grading_prompt_template: str,
    pricing: PricingSettings,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_grading_once_sync,
        solution=solution,
        rubric_json=rubric_json,
        grader=grader,
        grading_prompt_template=grading_prompt_template,
        pricing=pricing,
    )
