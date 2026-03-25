from __future__ import annotations

import uuid
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.chunk import Chunk
from app.schemas.chunking_schemas import ChunkCreate


class ChunkRepository:

    def __init__(self, db: AsyncSession):
        self.db = db

    # ── Create ────────────────────────────────────────────────────────────────

    async def create(self, data: ChunkCreate) -> Chunk:
        """Create a single chunk."""
        kwargs = dict(
            session_id=data.session_id,
            job_id=data.job_id,
            source_id=data.source_id,
            chunk_text=data.chunk_text,
            chunk_index=data.chunk_index,
            token_count=data.token_count,
            page_number=data.page_number,
            section_title=data.section_title,
            heading_level=data.heading_level,
            language=data.language,
            lang_confidence=data.lang_confidence,
            chunk_strategy=data.chunk_strategy,
            parent_chunk_id=data.parent_chunk_id,
            topic=data.topic,
            doc_type=data.doc_type,
            entities=data.entities,
            keywords=data.keywords,
        )
        if data.id is not None:
            kwargs["id"] = data.id
        record = Chunk(**kwargs)
        self.db.add(record)
        await self.db.flush()
        return record

    async def create_many(self, items: list[ChunkCreate]) -> list[Chunk]:
        """Bulk insert chunks."""
        records = []
        for d in items:
            kwargs = dict(
                session_id=d.session_id,
                job_id=d.job_id,
                source_id=d.source_id,
                chunk_text=d.chunk_text,
                chunk_index=d.chunk_index,
                token_count=d.token_count,
                page_number=d.page_number,
                section_title=d.section_title,
                heading_level=d.heading_level,
                language=d.language,
                lang_confidence=d.lang_confidence,
                chunk_strategy=d.chunk_strategy,
                parent_chunk_id=d.parent_chunk_id,
                topic=d.topic,
                doc_type=d.doc_type,
                entities=d.entities,
                keywords=d.keywords,
            )
            if d.id is not None:
                kwargs["id"] = d.id
            records.append(Chunk(**kwargs))
        self.db.add_all(records)
        await self.db.flush()
        return records

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_by_id(
        self,
        chunk_id: uuid.UUID,
        session_id: uuid.UUID | None = None,
    ) -> Chunk | None:
        query = select(Chunk).where(Chunk.id == chunk_id)
        if session_id is not None:
            query = query.where(Chunk.session_id == session_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_by_job_id(
        self,
        job_id: uuid.UUID,
        session_id: uuid.UUID | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Chunk]:
        """Get all chunks for a job."""
        query = select(Chunk).where(Chunk.job_id == job_id)
        if session_id is not None:
            query = query.where(Chunk.session_id == session_id)
        query = query.order_by(Chunk.chunk_index).limit(limit).offset(offset)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def count_by_job(self, job_id: uuid.UUID, session_id: uuid.UUID | None = None) -> int:
        """Count chunks for a job."""
        query = select(func.count()).select_from(Chunk).where(Chunk.job_id == job_id)
        if session_id is not None:
            query = query.where(Chunk.session_id == session_id)
        result = await self.db.execute(query)
        return result.scalar_one() or 0

    async def delete_by_job_id(self, job_id: uuid.UUID, session_id: uuid.UUID | None = None) -> int:
        """Delete all chunks for a job."""
        query = delete(Chunk).where(Chunk.job_id == job_id)
        if session_id is not None:
            query = query.where(Chunk.session_id == session_id)
        result = await self.db.execute(query)
        await self.db.flush()
        return result.rowcount or 0

    async def delete_by_document_id(self, document_id: uuid.UUID, session_id: uuid.UUID | None = None) -> int:
        """Delete all chunks for a specific document (source_id)."""
        query = delete(Chunk).where(Chunk.source_id == document_id)
        if session_id is not None:
            query = query.where(Chunk.session_id == session_id)
        result = await self.db.execute(query)
        await self.db.flush()
        return result.rowcount or 0

    async def delete_by_id(self, chunk_id: uuid.UUID, session_id: uuid.UUID | None = None) -> bool:
        """Delete a single chunk by its ID."""
        chunk = await self.get_by_id(chunk_id=chunk_id, session_id=session_id)
        if not chunk:
            return False
        await self.db.delete(chunk)
        await self.db.flush()
        return True