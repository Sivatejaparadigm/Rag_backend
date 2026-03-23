from __future__ import annotations

import unicodedata
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.pipeline.preprocessor.base import BasePreprocessor
from app.pipeline.preprocessor.steps import (
    EncodingFixStep,
    NormalisationStep,
    WhitespaceStep,
    QualityFilterStep,
    LanguageDetectionStep,
    DeduplicationStep,
)
from app.repositories.job_repository import JobRepository
from app.repositories.preprocessor_repository import PreprocessedDataRepository
from app.schemas.preprocessor import (
    PreprocessedDataCreate,
    PreprocessedDataUpdate,
    PreprocessingResult,
    PreprocessStatus,
)

logger = logging.getLogger(__name__)


class PreprocessingPipeline:
    """
    Preprocessing pipeline for documents after extraction.

    Pipeline order:
        raw_text (from ExtractedContent)
            → EncodingFixStep        ftfy.fix_text()
            → NormalisationStep      unicodedata.normalize('NFKC') + control char strip
            → WhitespaceStep         zero-width chars, multi-space, multi-newline
            → QualityFilterStep      junk / noise filter (min words, symbol ratio)
            → LanguageDetectionStep  fastText lid.176.bin → lingua fallback
            → DeduplicationStep      BLAKE3 exact hash → MinHash LSH near-dup
            → preprocessed_text      ready for Chunking Engine

    Usage:
        pipeline = PreprocessingPipeline(job_repo=job_repo, db=db)
        result   = await pipeline.run(job_id=job_id, tenant_id=tenant_id)
    """

    def __init__(
        self,
        job_repo: JobRepository,
        db: AsyncSession,
        steps: list[BasePreprocessor] | None = None,
    ) -> None:
        self.job_repo           = job_repo
        self.db                 = db
        self.preprocessed_repo  = PreprocessedDataRepository(db)

        # Default step order — can be overridden for testing or custom tenants
        self.steps: list[BasePreprocessor] = steps or [
            EncodingFixStep(),
            NormalisationStep(),
            WhitespaceStep(),
            QualityFilterStep(),
            LanguageDetectionStep(),
            DeduplicationStep(),
        ]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _apply_steps(self, text: str) -> PreprocessingResult:
        """
        Run each step in order.
        Any step may raise StopPreprocessing to short-circuit the pipeline
        (e.g. QualityFilterStep on junk, DeduplicationStep on duplicate).
        """
        current = text

        for step in self.steps:
            result = step.process(current)

            if not result.passed:
                # Step signalled rejection or duplication — stop here
                return result

            current = result.preprocessed_text

        return PreprocessingResult(
            preprocessed_text=current,
            passed=True,
        )

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(
        self,
        job_id,
        tenant_id,
    ) -> dict:
        """
        Load ExtractedContent for job_id, run the full pipeline,
        persist the result into preprocessed_data.

        Returns:
            {
                "record":   PreprocessedData ORM instance,
                "status":   PreprocessStatus,
                "message":  str,
            }
        """
        # ── 1. Load source job + extracted content ────────────────────────────
        job = await self.job_repo.get_job(job_id=job_id, tenant_id=tenant_id)
        if not job:
            raise ValueError(f"IngestionJob not found: job_id={job_id}")

        if not job.content:
            raise ValueError(f"No ExtractedContent for job_id={job_id}")

        content   = job.content
        raw_text  = content.raw_text or ""

        # ── 2. Check if a preprocessed record already exists ──────────────────
        existing = await self.preprocessed_repo.get_by_job_id(job_id=job_id)

        # ── 3. Run pipeline steps ─────────────────────────────────────────────
        try:
            result = self._apply_steps(raw_text)
        except Exception as exc:
            logger.exception("Preprocessing pipeline error for job_id=%s", job_id)
            error_msg = str(exc)

            if existing:
                await self.preprocessed_repo.mark_failed(
                    record_id=existing.id,
                    error=error_msg,
                )
            else:
                await self.preprocessed_repo.create(
                    PreprocessedDataCreate(
                        tenant_id=tenant_id,
                        job_id=job_id,
                        content_id=content.id,
                        filename=job.filename,
                        document_type=job.document_type,
                        source_type=job.source_type,
                        source_uri=job.source_uri,
                        raw_text=raw_text,
                        preprocessed_text=None,
                        status=PreprocessStatus.FAILED,
                        error_message=error_msg,
                    )
                )

            await self.db.commit()
            return {
                "record":  None,
                "status":  PreprocessStatus.FAILED,
                "message": f"Pipeline failed: {error_msg}",
            }

        # ── 4. Map result → status ────────────────────────────────────────────
        if not result.passed:
            # Determine whether rejection or duplicate
            status = (
                PreprocessStatus.SKIPPED_DUP
                if getattr(result, "is_duplicate", False)
                else PreprocessStatus.REJECTED
            )
        else:
            status = PreprocessStatus.COMPLETED

        # ── 5. Persist ────────────────────────────────────────────────────────
        if existing:
            await self.preprocessed_repo.update(
                record_id=existing.id,
                data=PreprocessedDataUpdate(
                    preprocessed_text=result.preprocessed_text,
                    status=status,
                    error_message=None,
                ),
            )
            record = await self.preprocessed_repo.get_by_id(existing.id)
        else:
            record = await self.preprocessed_repo.create(
                PreprocessedDataCreate(
                    tenant_id=tenant_id,
                    job_id=job_id,
                    content_id=content.id,
                    filename=job.filename,
                    document_type=job.document_type,
                    source_type=job.source_type,
                    source_uri=job.source_uri,
                    raw_text=raw_text,
                    preprocessed_text=result.preprocessed_text,
                    status=status,
                    error_message=None,
                )
            )

        await self.db.commit()

        logger.info(
            "Preprocessing complete: job_id=%s status=%s",
            job_id,
            status.value,
        )

        return {
            "record":  record,
            "status":  status,
            "message": f"Preprocessing {status.value}",
        }
