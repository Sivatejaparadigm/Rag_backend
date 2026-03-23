from __future__ import annotations

import uuid
from pathlib import Path

from app.pipeline.extractors.base import BaseExtractor, ExtractionError
from app.schemas.ingestion import DocumentType, ExtractedContentCreate


class DocxExtractor(BaseExtractor):
    supported_type = DocumentType.DOCX

    async def extract(self, file_path: Path, tenant_id: uuid.UUID) -> ExtractedContentCreate:
        raise ExtractionError("DOCX extraction is not implemented yet.")

