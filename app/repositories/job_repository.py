from __future__ import annotations

import uuid
from datetime import datetime,timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.ingestion import ExtractedContent, IngestionJob


class JobRepository:

    def __init__(self, db: AsyncSession):
        self.db = db

    #Job Creation

    async def create_job(self, filename: str, document_type: str, session_id: uuid.UUID) -> IngestionJob:
        job = IngestionJob(
            filename=filename,
            document_type=document_type,
            session_id=session_id,
            status="pending",
        )
        self.db.add(job)
        await self.db.flush()   # writes to DB, gets the id, no commit yet
        return job

    # Job Read

    async def get_job(self, job_id: uuid.UUID, session_id: uuid.UUID | None = None) -> IngestionJob | None:
        query = select(IngestionJob).where(IngestionJob.id == job_id)
        if session_id is not None:
            query = query.where(IngestionJob.session_id == session_id)
        query = query.options(selectinload(IngestionJob.content))  # load content in same query
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_jobs(
        self,
        session_id: uuid.UUID | None = None,
        status: str = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[IngestionJob]:
        query = (
            select(IngestionJob)
            .options(selectinload(IngestionJob.content))
            .order_by(IngestionJob.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if session_id is not None:
            query = query.where(IngestionJob.session_id == session_id)
        if status:
            query = query.where(IngestionJob.status == status)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def count_jobs(self, session_id: uuid.UUID | None = None, status: str = None) -> int:
        from sqlalchemy import func
        query = select(func.count()).select_from(IngestionJob)
        if session_id is not None:
            query = query.where(IngestionJob.session_id == session_id)
        if status:
            query = query.where(IngestionJob.status == status)
        result = await self.db.execute(query)
        return result.scalar_one()

    # Status Updates

    async def mark_processing(self, job_id: uuid.UUID) -> None:
        await self.db.execute(
            update(IngestionJob)
            .where(IngestionJob.id == job_id)
            .values(
                status="processing",
                updated_at=datetime.now(timezone.utc),
            )
        )

    async def mark_completed(
        self,
        job_id: uuid.UUID,
        word_count: int,
        page_count: int | None,
    ) -> None:
        await self.db.execute(
            update(IngestionJob)
            .where(IngestionJob.id == job_id)
            .values(
                status="completed",
                word_count=word_count,
                page_count=page_count,
                updated_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
        )

    async def mark_failed(
        self,
        job_id: uuid.UUID,
        error: str,
        retry_count: int,
    ) -> None:
        await self.db.execute(
            update(IngestionJob)
            .where(IngestionJob.id == job_id)
            .values(
                status="failed",
                error_message=error,
                retry_count=retry_count,
                updated_at=datetime.now(timezone.utc),
            )
        )

    async def mark_retrying(
        self,
        job_id: uuid.UUID,
        retry_count: int,
    ) -> None:
        await self.db.execute(
            update(IngestionJob)
            .where(IngestionJob.id == job_id)
            .values(
                status="retrying",
                retry_count=retry_count,
                updated_at=datetime.now(timezone.utc),
            )
        )

    # Content Saving

    async def save_content(
        self,
        job_id: uuid.UUID,
        session_id: uuid.UUID,
        raw_text: str,
        pages: list,
        tables: list,
        warnings: list,
    ) -> ExtractedContent:
        content = ExtractedContent(
            job_id=job_id,
            session_id=session_id,
            raw_text=raw_text,
            pages=pages,
            tables=tables,
            warnings=warnings,
        )
        self.db.add(content)
        await self.db.flush()
        return content

    # Job Deletion

    async def delete_job(self, job_id: uuid.UUID) -> bool:
        job = await self.get_job(job_id)
        if not job:
            return False
        await self.db.delete(job)   # cascade deletes extracted_content too
        await self.db.flush()
        return True
