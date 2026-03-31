from __future__ import annotations

from dataclasses import dataclass
import json
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True, slots=True)
class OpenAIModelPricing:
    input_per_1m_usd: float
    cached_input_per_1m_usd: float
    output_per_1m_usd: float


@dataclass(frozen=True, slots=True)
class OpenAIPriceBook:
    price_book_id: str
    models: dict[str, OpenAIModelPricing]


@lru_cache(maxsize=8)
def load_openai_price_book(path: str | Path) -> OpenAIPriceBook:
    source = Path(path).resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    models: dict[str, OpenAIModelPricing] = {}
    for model_id, item in payload.get("models", {}).items():
        models[str(model_id).lower()] = OpenAIModelPricing(
            input_per_1m_usd=float(item["input_per_1m_usd"]),
            cached_input_per_1m_usd=float(item["cached_input_per_1m_usd"]),
            output_per_1m_usd=float(item["output_per_1m_usd"]),
        )
    return OpenAIPriceBook(
        price_book_id=str(payload.get("price_book_id") or source.stem),
        models=models,
    )


def openai_cost_usd(
    price_book_path: str | Path,
    *,
    model_id: str,
    input_tokens: int,
    cached_input_tokens: int,
    output_tokens: int,
) -> float | None:
    price_book = load_openai_price_book(price_book_path)
    rates = price_book.models.get((model_id or "").lower())
    if not rates:
        return None
    non_cached_input_tokens = max(int(input_tokens or 0) - int(cached_input_tokens or 0), 0)
    return (
        (non_cached_input_tokens / 1_000_000.0) * rates.input_per_1m_usd
        + (int(cached_input_tokens or 0) / 1_000_000.0) * rates.cached_input_per_1m_usd
        + (int(output_tokens or 0) / 1_000_000.0) * rates.output_per_1m_usd
    )


def openai_price_book_id(path: str | Path) -> str:
    return load_openai_price_book(path).price_book_id
