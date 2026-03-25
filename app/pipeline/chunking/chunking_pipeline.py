from __future__ import annotations

import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.pipeline.chunking.base import BaseChunker
from app.pipeline.chunking.steps import (
    FixedSizeChunker,
    RecursiveChunker,
    SemanticChunker,
    AgenticChunker,
    ParentChildChunker,
)
from app.repositories.job_repository import JobRepository
from app.repositories.preprocessor_repository import PreprocessedDataRepository
from app.repositories.chunk_repository import ChunkRepository
from app.schemas.chunking_schemas import (
    ChunkCreate,
    ChunkingResult,
    ChunkStatus,
    ChunkStrategy,
    FixedSizeConfig,
    RecursiveConfig,
    SemanticConfig,
    AgenticConfig,
    ParentChildConfig,
)

logger = logging.getLogger(__name__)


class ChunkingPipeline:
    """
    Chunking pipeline — runs directly after PreprocessingPipeline.

    Reads the preprocessed_pages produced by PreprocessingPipeline,
    applies the requested chunking strategy, and persists all chunks
    into the chunks table via ChunkRepository.

    Pipeline order:
        preprocessed_pages  (from preprocessed_data)
            → strategy step    FixedSizeChunker | RecursiveChunker |
                               SemanticChunker  | AgenticChunker   |
                               ParentChildChunker
            → ChunkRepository.create_many()
            → chunks           ready for EmbeddingPipeline

    Strategy is selected at runtime from the ChunkRequest —
    job_id + session_id are sufficient to resolve the source document.
    document_id is optional: when provided, only that document is chunked.

    Usage:
        pipeline = ChunkingPipeline(
            job_repo    = job_repo,
            db          = db,
            strategy    = ChunkStrategy.RECURSIVE,
        )
        result = await pipeline.run(job_id=job_id, session_id=session_id)
    """

    # Strategy → chunker class mapping
    _STRATEGY_MAP: dict[ChunkStrategy, type[BaseChunker]] = {
        ChunkStrategy.FIXED:        FixedSizeChunker,
        ChunkStrategy.RECURSIVE:    RecursiveChunker,
        ChunkStrategy.SEMANTIC:     SemanticChunker,
        ChunkStrategy.AGENTIC:      AgenticChunker,
        ChunkStrategy.PARENT_CHILD: ParentChildChunker,
    }

    def __init__(
        self,
        job_repo:  JobRepository,
        db:        AsyncSession,
        strategy:  ChunkStrategy                      = ChunkStrategy.RECURSIVE,
        config:    FixedSizeConfig
                 | RecursiveConfig
                 | SemanticConfig
                 | AgenticConfig
                 | ParentChildConfig
                 | None                               = None,
    ) -> None:
        self.job_repo          = job_repo
        self.db                = db
        self.strategy          = strategy
        self.preprocessed_repo = PreprocessedDataRepository(db)
        self.chunk_repo        = ChunkRepository(db)

        # Instantiate the correct chunker with its config
        chunker_cls  = self._STRATEGY_MAP[strategy]
        self._chunker: BaseChunker = (
            chunker_cls(config) if config is not None else chunker_cls()
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _apply_strategy(self, text: str) -> ChunkingResult:
        """
        Run the selected chunker.
        Returns ChunkingResult with passed=False on any failure —
        mirrors _apply_steps() in PreprocessingPipeline.
        """
        return self._chunker.chunk(text)

    def _build_chunk_creates(
        self,
        result:      ChunkingResult,
        session_id,
        job_id,
        document_id,
        filename:    str,
        file_type:   str,
        language:    str,
        lang_confidence: float,
        page_number: int,
    ) -> tuple[list[ChunkCreate], list[ChunkCreate]]:
        """
        Convert ChunkingResult items into ChunkCreate schemas
        ready for bulk insert.

        Returns (child_creates, parent_creates).
        parent_creates is non-empty only for ParentChildChunker.
        """
        def _make(item, total: int) -> ChunkCreate:
            return ChunkCreate(
                id              = uuid.UUID(item.id) if item.id else None,
                session_id       = session_id,
                job_id          = job_id,
                source_id       = document_id,
                chunk_text      = item.content,
                chunk_index     = item.chunk_index,
                token_count     = len(item.content) // 4,
                page_number     = page_number,
                language        = language,
                lang_confidence = lang_confidence,
                chunk_strategy  = self.strategy.value,
                parent_chunk_id = uuid.UUID(item.parent_chunk_id) if item.parent_chunk_id else None,
            )

        child_creates = [
            _make(item, len(result.chunks))
            for item in result.chunks
        ]
        parent_creates = [
            _make(item, len(result.parent_chunks))
            for item in (result.parent_chunks or [])
        ]
        return child_creates, parent_creates

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(
        self,
        job_id,
        session_id,
        document_id = None,
    ) -> dict:
        """
        Load preprocessed_pages for job_id, apply the chunking strategy,
        and persist all chunks.

        Args:
            job_id:      UUID of the job.
            session_id:   UUID of the tenant (multi-tenancy guard).
            document_id: Optional — scopes chunking to one document.
                         When omitted all preprocessed records for the
                         job are chunked (batch mode).

        Returns:
            {
                "chunks_saved":   int,
                "parents_saved":  int,
                "status":         ChunkStatus,
                "message":        str,
            }
        """

        # ── 1. Resolve preprocessed records ───────────────────────────────────
        if document_id is not None:
            preprocessed = await self.preprocessed_repo.get_by_content_id(
                content_id=document_id, session_id=session_id
            )
            records = [preprocessed] if preprocessed else []
        else:
            # Batch: get all preprocessed records for this job
            records = await self.preprocessed_repo.list_by_job_id(
                job_id=job_id, session_id=session_id
            )

        if not records:
            raise ValueError(
                f"No preprocessed records found for job_id={job_id}"
            )

        total_chunks_saved  = 0
        total_parents_saved = 0

        for record in records:
            if not record.preprocessed_pages:
                logger.warning(
                    "ChunkingPipeline: empty preprocessed_pages for record %s — skipping",
                    record.id,
                )
                continue

            # ── 2. Delete existing chunks before re-chunking ─────────────────
            await self.chunk_repo.delete_by_document_id(
                document_id=record.content_id,
                session_id=session_id,
            )

            for page in record.preprocessed_pages:
                text = page['text']
                page_number = page['page_number']
                language = page['language']
                lang_confidence = page['lang_confidence']

                if not text.strip():
                    logger.warning(
                        "ChunkingPipeline: empty text for page %d in record %s — skipping",
                        page_number, record.id,
                    )
                    continue

                # ── 3. Apply chunking strategy ────────────────────────────────────
                try:
                    result = self._apply_strategy(text)
                except Exception as exc:
                    logger.exception(
                        "ChunkingPipeline: strategy failed for record %s page %d", record.id, page_number
                    )
                    return {
                        "chunks_saved":  0,
                        "parents_saved": 0,
                        "status":        ChunkStatus.FAILED,
                        "message":       f"Chunking failed: {exc}",
                    }

                if not result.passed:
                    logger.warning(
                        "ChunkingPipeline: strategy returned passed=False for record %s page %d: %s",
                        record.id, page_number,
                        result.error_message,
                    )
                    return {
                        "chunks_saved":  0,
                        "parents_saved": 0,
                        "status":        ChunkStatus.FAILED,
                        "message":       result.error_message or "Chunking step failed",
                    }

                # ── 4. Build ChunkCreate objects ──────────────────────────────────
                child_creates, parent_creates = self._build_chunk_creates(
                    result          = result,
                    session_id       = session_id,
                    job_id          = job_id,
                    document_id     = record.content_id,
                    filename        = record.filename,
                    file_type       = record.document_type,
                    language        = language,
                    lang_confidence = lang_confidence,
                    page_number     = page_number,
                )

                # ── 5. Bulk save parents first (parent_child strategy only) ───────
                if parent_creates:
                    await self.chunk_repo.create_many(parent_creates)
                    total_parents_saved += len(parent_creates)

                # ── 6. Bulk save child / regular chunks ───────────────────────────
                if child_creates:
                    await self.chunk_repo.create_many(child_creates)
                    total_chunks_saved += len(child_creates)

        await self.db.commit()

        logger.info(
            "ChunkingPipeline complete: job_id=%s strategy=%s "
            "chunks=%d parents=%d",
            job_id,
            self.strategy.value,
            total_chunks_saved,
            total_parents_saved,
        )

        return {
            "chunks_saved":  total_chunks_saved,
            "parents_saved": total_parents_saved,
            "status":        ChunkStatus.CHUNKED,
            "message":       (
                f"Chunking completed — "
                f"{total_chunks_saved} chunks"
                + (f", {total_parents_saved} parents" if total_parents_saved else "")
            ),
        }
