from __future__ import annotations

import uuid
from pathlib import Path

from pypdf import PdfReader

from app.pipeline.extractors.base import BaseExtractor, ExtractionError
from app.schemas.ingestion import DocumentType, ExtractedContentCreate


class PDFExtractor(BaseExtractor):
    supported_type = DocumentType.PDF

    async def extract(self, file_path: Path, tenant_id: uuid.UUID) -> ExtractedContentCreate:
        try:
            self._ensure_file_exists(file_path)
            reader = PdfReader(str(file_path))
        except Exception as e:  # noqa: BLE001
            raise ExtractionError("Failed to read PDF", original=e) from e

        pages: list[str] = []
        text_parts: list[str] = []

        for page in reader.pages:
            page_text = page.extract_text() or ""
            page_text = self._truncate(page_text)
            pages.append(page_text)
            text_parts.append(page_text)

        raw_text = self._truncate("\n".join(text_parts))
        return ExtractedContentCreate(
            tenant_id=tenant_id,
            raw_text=raw_text,
            pages=pages,
            tables=[],
            warnings=[],
        )

