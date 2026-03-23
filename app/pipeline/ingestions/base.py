from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from pathlib import Path

from app.pipeline.extractors.base import BaseExtractor
from app.repositories.job_repository import JobRepository
from app.schemas.ingestion import DocumentType, ExtractedContentCreate


class BaseIngestion(ABC):
    """
    Base class that orchestrates ingestion for a single document.

    Today it supports: extract -> persist extracted content -> update job stats.
    Later you can extend it with preprocessing/chunking/vectorization steps.
    """

    def __init__(self, job_repo: JobRepository) -> None:
        self.job_repo = job_repo

    @abstractmethod
    def get_extractor(self, document_type: DocumentType) -> BaseExtractor:
        raise NotImplementedError

    async def extract(
        self, *, file_path: Path, tenant_id: uuid.UUID, document_type: DocumentType
    ) -> ExtractedContentCreate:
        extractor = self.get_extractor(document_type)
        return await extractor.extract(file_path=file_path, tenant_id=tenant_id)

    async def preprocess(self, extracted: ExtractedContentCreate) -> str:
        """
        Hook for text preprocessing. Default: pass through `raw_text`.
        """

        return extracted.raw_text or ""

    async def chunk(self, preprocessed_text: str) -> list[str]:
        """
        Hook for chunking. Default: single chunk for now.
        """

        return [preprocessed_text] if preprocessed_text else []

    async def persist(self, *, job_id: uuid.UUID, tenant_id: uuid.UUID, extracted: ExtractedContentCreate, chunks: list[str]) -> None:
        # For now we only persist extracted content (no chunk table yet).
        await self.job_repo.save_content(
            job_id=job_id,
            tenant_id=tenant_id,
            raw_text=extracted.raw_text or "",
            pages=extracted.pages or [],
            tables=extracted.tables or [],
            warnings=extracted.warnings or [],
        )

    async def run(
        self, *, job_id: uuid.UUID, file_path: Path, tenant_id: uuid.UUID, document_type: DocumentType
    ) -> None:
        await self.job_repo.mark_processing(job_id)

        try:
            extracted = await self.extract(file_path=file_path, tenant_id=tenant_id, document_type=document_type)
            preprocessed_text = await self.preprocess(extracted)
            chunks = await self.chunk(preprocessed_text)
            await self.persist(job_id=job_id, tenant_id=tenant_id, extracted=extracted, chunks=chunks)

            word_count = len(preprocessed_text.split()) if preprocessed_text.strip() else 0
            page_count = len(extracted.pages or [])
            await self.job_repo.mark_completed(job_id=job_id, word_count=word_count, page_count=page_count)
        except Exception as e:  # noqa: BLE001
            # Keep it simple for now; later you can add retry logic.
            await self.job_repo.mark_failed(job_id=job_id, error=str(e), retry_count=0)
            raise

