from __future__ import annotations

import logging

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
            → LanguageDetectionStep  lingua-py language detection
            → DeduplicationStep      SHA-256 exact hash → MinHash LSH near-dup
            → preprocessed_text      ready for Chunking Engine

    Usage:
        pipeline = PreprocessingPipeline(job_repo=job_repo, db=db)
        result   = await pipeline.run(job_id=job_id, session_id=session_id)
    """

    def __init__(
        self,
        job_repo: JobRepository,
        db:       AsyncSession,
        steps:    list[BasePreprocessor] | None = None,
    ) -> None:
        self.job_repo          = job_repo
        self.db                = db
        self.preprocessed_repo = PreprocessedDataRepository(db)

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

    def _apply_steps(self, pages: list[dict]) -> PreprocessingResult:
        """
        Two-phase preprocessing:

        Phase 1 — per-page cleaning (Encoding, Normalisation, Whitespace,
                   QualityFilter, LanguageDetection).
                   A page that fails QualityFilter is skipped — not the whole doc.

        Phase 2 — document-level deduplication.
                   Runs ONCE on the combined text of all cleaned pages.
                   Compares the whole document against other whole documents.
        """

        # ── Phase 1: per-page cleaning steps ─────────────────────────────────
        # DeduplicationStep is excluded here — it runs in Phase 2.
        cleaning_steps = [
            step for step in self.steps
            if not isinstance(step, DeduplicationStep)
        ]

        preprocessed_pages = []
        metadata = {
            "language":        "UNKNOWN",
            "lang_confidence": 0.0,
            "is_duplicate":    False,
        }

        for page in pages:
            current       = page["text"]
            page_metadata = metadata.copy()

            for step in cleaning_steps:
                result = step.process(current)

                if not result.passed:
                    # QualityFilterStep rejected this page — skip the page only,
                    # not the whole document.
                    logger.debug(
                        "_apply_steps: page %s rejected by %s — skipping page",
                        page.get("page_number", "?"),
                        step.__class__.__name__,
                    )
                    current = None
                    break

                # Carry language metadata forward from LanguageDetectionStep
                if getattr(result, "language", "UNKNOWN") != "UNKNOWN":
                    page_metadata["language"]       = result.language
                    page_metadata["lang_confidence"] = result.lang_confidence

                current = result.preprocessed_text

            # Page was rejected by a cleaning step — do not include it
            if current is None:
                continue

            preprocessed_pages.append({
                **page,
                "text":            current,
                "language":        page_metadata["language"],
                "lang_confidence": page_metadata["lang_confidence"],
            })

            # Keep the highest-confidence language detection across all pages
            if page_metadata["lang_confidence"] > metadata["lang_confidence"]:
                metadata["language"]       = page_metadata["language"]
                metadata["lang_confidence"] = page_metadata["lang_confidence"]

        # If every page was rejected by QualityFilter, stop here
        if not preprocessed_pages:
            logger.warning("_apply_steps: all pages rejected by quality filter")
            return PreprocessingResult(
                preprocessed_text="",
                preprocessed_pages=[],
                language=metadata["language"],
                lang_confidence=metadata["lang_confidence"],
                is_duplicate=False,
                passed=False,
            )

        # ── Phase 2: document-level deduplication ─────────────────────────────
        # Runs ONCE on the full combined text of all cleaned pages.
        # MinHash is computed on the whole document — not per page.
        dedup_step = next(
            (step for step in self.steps if isinstance(step, DeduplicationStep)),
            None,
        )

        if dedup_step is not None:
            combined_text = "\n\n".join(p["text"] for p in preprocessed_pages)
            dedup_result  = dedup_step.process(combined_text)

            if not dedup_result.passed:
                logger.info(
                    "_apply_steps: document rejected as duplicate (is_duplicate=%s)",
                    dedup_result.is_duplicate,
                )
                return PreprocessingResult(
                    preprocessed_text="",
                    preprocessed_pages=[],
                    language=metadata["language"],
                    lang_confidence=metadata["lang_confidence"],
                    is_duplicate=True,
                    passed=False,
                )

        # ── All steps passed — return cleaned pages ───────────────────────────
        combined_text = "\n\n".join(p["text"] for p in preprocessed_pages)

        return PreprocessingResult(
            preprocessed_text=combined_text,
            preprocessed_pages=preprocessed_pages,
            language=metadata["language"],
            lang_confidence=metadata["lang_confidence"],
            is_duplicate=False,
            passed=True,
        )

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(
        self,
        job_id,
        session_id,
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
        job = await self.job_repo.get_job(job_id=job_id, session_id=session_id)
        if not job:
            raise ValueError(f"IngestionJob not found: job_id={job_id}")

        if not job.content:
            raise ValueError(f"No ExtractedContent for job_id={job_id}")

        content = job.content
        pages   = content.pages or []

        # ── 2. Check if a preprocessed record already exists ──────────────────
        existing = await self.preprocessed_repo.get_by_job_id(job_id=job_id)

        # ── 3. Run pipeline steps ─────────────────────────────────────────────
        try:
            result = self._apply_steps(pages)
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
                        session_id=session_id,
                        job_id=job_id,
                        content_id=content.id,
                        filename=job.filename,
                        document_type=job.document_type,
                        source_type=job.source_type,
                        source_uri=job.source_uri,
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
                    preprocessed_pages=result.preprocessed_pages,
                    language=result.language,
                    lang_confidence=result.lang_confidence,
                    status=status,
                    error_message=None,
                ),
            )
            record = await self.preprocessed_repo.get_by_id(existing.id)
        else:
            record = await self.preprocessed_repo.create(
                PreprocessedDataCreate(
                    session_id=session_id,
                    job_id=job_id,
                    content_id=content.id,
                    filename=job.filename,
                    document_type=job.document_type,
                    source_type=job.source_type,
                    source_uri=job.source_uri,
                    preprocessed_text=result.preprocessed_text,
                    preprocessed_pages=result.preprocessed_pages,
                    language=result.language,
                    lang_confidence=result.lang_confidence,
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