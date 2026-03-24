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
<<<<<<< HEAD
            # 1) Persist raw extracted content first — preprocessing loads from
            #    `extracted_contents` (via the job.content relationship).
            await self.persist(
                job_id=job_id,
                tenant_id=tenant_id,
                extracted=extracted,
                chunks=[],
            )

            # 2) Run full preprocessing pipeline and persist into `preprocessed_data`.
            from app.pipeline.preprocessor.preprocessing_pipeline import PreprocessingPipeline

            pipeline = PreprocessingPipeline(job_repo=self.job_repo, db=self.job_repo.db)
            preprocessing_result = await pipeline.run(job_id=job_id, tenant_id=tenant_id)

            # 3) Use preprocessed text for job stats + (future) chunking.
            status = preprocessing_result.get("status")
            record = preprocessing_result.get("record")
            if getattr(status, "value", None) == "failed":
                await self.job_repo.mark_failed(
                    job_id=job_id,
                    error=preprocessing_result.get("message") or "Preprocessing failed",
                    retry_count=0,
                )
                return

            preprocessed_text = ""
            if getattr(status, "value", None) == "completed" and record and record.preprocessed_text:
                preprocessed_text = record.preprocessed_text

            chunks = await self.chunk(preprocessed_text)
=======
            preprocessed_text = await self.preprocess(extracted)
            chunks = await self.chunk(preprocessed_text)
            await self.persist(job_id=job_id, tenant_id=tenant_id, extracted=extracted, chunks=chunks)

>>>>>>> f425f686a1d9fa7ceb4ac42affb0b118e08c77a3
            word_count = len(preprocessed_text.split()) if preprocessed_text.strip() else 0
            page_count = len(extracted.pages or [])
            await self.job_repo.mark_completed(job_id=job_id, word_count=word_count, page_count=page_count)
        except Exception as e:  # noqa: BLE001
            # Keep it simple for now; later you can add retry logic.
            await self.job_repo.mark_failed(job_id=job_id, error=str(e), retry_count=0)
            raise

