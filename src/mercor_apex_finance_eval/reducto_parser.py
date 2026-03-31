from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from .config import ReductoSettings
from .types import ParsedAttachment
from .utils import ensure_dir, sha256_bytes, utc_now_iso, write_json


class ReductoAttachmentParser:
    def __init__(self, *, settings: ReductoSettings, cache_dir: str | Path) -> None:
        self.settings = settings
        self.cache_dir = ensure_dir(Path(cache_dir))

    def _lazy_client(self):
        try:
            from reducto import Reducto
        except ImportError as exc:
            raise RuntimeError(
                "Could not import Reducto SDK. Install dependencies with `pip install -r requirements.txt`."
            ) from exc
        return Reducto()

    def _parse_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "formatting": {"table_output_format": self.settings.table_output_format},
            "retrieval": {"chunking": {"chunk_mode": self.settings.chunk_mode}},
            "settings": {},
            "enhance": {},
        }
        if self.settings.filter_blocks:
            kwargs["retrieval"]["filter_blocks"] = self.settings.filter_blocks
        if self.settings.page_start is not None or self.settings.page_end is not None:
            kwargs["settings"]["page_range"] = {
                "start": self.settings.page_start or 1,
                "end": self.settings.page_end,
            }
        if self.settings.summarize_figures:
            kwargs["enhance"]["summarize_figures"] = True
        if self.settings.agentic_scopes:
            kwargs["enhance"]["agentic"] = [{"scope": scope} for scope in self.settings.agentic_scopes]

        # Remove empty sections to keep requests tidy.
        return {key: value for key, value in kwargs.items() if value}

    def _cache_path(self, file_path: Path, parse_kwargs: dict[str, Any]) -> Path:
        digest = sha256_bytes(file_path.read_bytes() + json.dumps(parse_kwargs, sort_keys=True).encode("utf-8"))
        return self.cache_dir / f"{digest}.json"

    def _coerce_chunks_to_text(self, chunks: Any) -> str:
        if isinstance(chunks, dict):
            if "chunks" in chunks:
                return self._coerce_chunks_to_text(chunks["chunks"])
            return json.dumps(chunks, ensure_ascii=False)
        if isinstance(chunks, list):
            parts: list[str] = []
            for item in chunks:
                if isinstance(item, dict):
                    content = item.get("content") or item.get("embed") or json.dumps(item, ensure_ascii=False)
                else:
                    content = getattr(item, "content", None) or getattr(item, "embed", None) or str(item)
                parts.append(content)
            return "\n\n".join(part for part in parts if part)
        return str(chunks)

    def _extract_content(self, result: Any) -> str:
        result_obj = getattr(result, "result", None)
        result_type = getattr(result_obj, "type", None)
        if result_type == "url":
            url = getattr(result_obj, "url", None)
            if not url:
                raise RuntimeError("Reducto returned a URL result without a URL.")
            with urlopen(url) as response:  # nosec - trusted API URL from provider
                payload = json.loads(response.read().decode("utf-8"))
            return self._coerce_chunks_to_text(payload)
        chunks = getattr(result_obj, "chunks", None)
        return self._coerce_chunks_to_text(chunks)

    def parse_file(self, file_path: str | Path) -> ParsedAttachment:
        file_path = Path(file_path)
        parse_kwargs = self._parse_kwargs()
        cache_path = self._cache_path(file_path, parse_kwargs)

        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            return ParsedAttachment(
                filename=file_path.name,
                relative_path=str(file_path),
                content=cached["content"],
                cache_hit=True,
                num_pages=cached["metadata"].get("num_pages"),
                credits_incurred=0.0,
                cost_incurred_usd=0.0,
                duration_seconds=cached["metadata"].get("duration_seconds"),
                studio_link=cached["metadata"].get("studio_link"),
                job_id=cached["metadata"].get("job_id"),
                metadata=cached["metadata"],
            )

        client = self._lazy_client()
        upload = client.upload(file=file_path)
        result = client.parse.run(input=upload.file_id, **parse_kwargs)
        content = self._extract_content(result)

        credits = float(getattr(getattr(result, "usage", None), "credits", 0.0) or 0.0)
        num_pages = getattr(getattr(result, "usage", None), "num_pages", None)
        duration = getattr(result, "duration", None)
        studio_link = getattr(result, "studio_link", None)
        job_id = getattr(result, "job_id", None)

        metadata = {
            "source_file": str(file_path),
            "num_pages": num_pages,
            "credits": credits,
            "duration_seconds": duration,
            "studio_link": studio_link,
            "job_id": job_id,
            "cached_at": utc_now_iso(),
            "parse_kwargs": parse_kwargs,
        }
        write_json(cache_path, {"content": content, "metadata": metadata})

        return ParsedAttachment(
            filename=file_path.name,
            relative_path=str(file_path),
            content=content,
            cache_hit=False,
            num_pages=num_pages,
            credits_incurred=credits,
            cost_incurred_usd=credits * self.settings.credit_price_usd,
            duration_seconds=duration,
            studio_link=studio_link,
            job_id=job_id,
            metadata=metadata,
        )

    def parse_many(self, file_paths: list[Path]) -> list[ParsedAttachment]:
        if not self.settings.enabled:
            return []
        return [self.parse_file(path) for path in file_paths]
