from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.preprocessor import PreprocessedData
from app.schemas.preprocessor import (
    PreprocessedDataCreate,
    PreprocessedDataUpdate,
    PreprocessStatus,
)


class PreprocessedDataRepository:

    def __init__(self, db: AsyncSession):
        self.db = db

    # ── Create ────────────────────────────────────────────────────────────────

    async def create(self, data: PreprocessedDataCreate) -> PreprocessedData:
        record = PreprocessedData(
            tenant_id=data.tenant_id,
            job_id=data.job_id,
            content_id=data.content_id,
            filename=data.filename,
            document_type=data.document_type,
            source_type=data.source_type,
            source_uri=data.source_uri,
            preprocessed_text=data.preprocessed_text,
            preprocessed_pages=data.preprocessed_pages,
            language=data.language,
            lang_confidence=data.lang_confidence,
            status=data.status,
            error_message=data.error_message,
        )
        self.db.add(record)
        await self.db.flush()   # writes to DB, gets the id, no commit yet
        return record

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_by_id(
        self,
        record_id: uuid.UUID,
        tenant_id: uuid.UUID | None = None,
    ) -> PreprocessedData | None:
        query = select(PreprocessedData).where(PreprocessedData.id == record_id)
        if tenant_id is not None:
            query = query.where(PreprocessedData.tenant_id == tenant_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_by_job_id(
        self,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID | None = None,
    ) -> PreprocessedData | None:
        """Return the preprocessed record for a given ingestion job."""
        query = select(PreprocessedData).where(PreprocessedData.job_id == job_id)
        if tenant_id is not None:
            query = query.where(PreprocessedData.tenant_id == tenant_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_by_job_id(
        self,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID | None = None,
    ) -> list[PreprocessedData]:
        """Return all preprocessed records for a given ingestion job."""
        query = select(PreprocessedData).where(PreprocessedData.job_id == job_id)
        if tenant_id is not None:
            query = query.where(PreprocessedData.tenant_id == tenant_id)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_by_content_id(
        self,
        content_id: uuid.UUID,
        tenant_id: uuid.UUID | None = None,
    ) -> PreprocessedData | None:
        """Return the preprocessed record for a given extracted_contents row."""
        query = select(PreprocessedData).where(PreprocessedData.content_id == content_id)
        if tenant_id is not None:
            query = query.where(PreprocessedData.tenant_id == tenant_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        status: PreprocessStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[PreprocessedData]:
        query = (
            select(PreprocessedData)
            .where(PreprocessedData.tenant_id == tenant_id)
            .order_by(PreprocessedData.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if status is not None:
            query = query.where(PreprocessedData.status == status)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def count_by_tenant(
        self,
        tenant_id: uuid.UUID,
        status: PreprocessStatus | None = None,
    ) -> int:
        query = (
            select(func.count())
            .select_from(PreprocessedData)
            .where(PreprocessedData.tenant_id == tenant_id)
        )
        if status is not None:
            query = query.where(PreprocessedData.status == status)
        result = await self.db.execute(query)
        return result.scalar_one()

    # ── Update ────────────────────────────────────────────────────────────────

    async def update(
        self,
        record_id: uuid.UUID,
        data: PreprocessedDataUpdate,
    ) -> None:
        """
        Partial update — only sets fields that are explicitly provided.
        Used when re-running preprocessing on an existing record.
        """
        values: dict = {"updated_at": datetime.utcnow()}

        if data.preprocessed_text is not None:
            values["preprocessed_text"] = data.preprocessed_text
        if data.preprocessed_pages is not None:
            values["preprocessed_pages"] = data.preprocessed_pages
        if data.language is not None:
            values["language"] = data.language
        if data.lang_confidence is not None:
            values["lang_confidence"] = data.lang_confidence
        if data.status is not None:
            values["status"] = data.status
        if data.error_message is not None:
            values["error_message"] = data.error_message

        await self.db.execute(
            update(PreprocessedData)
            .where(PreprocessedData.id == record_id)
            .values(**values)
        )

    async def mark_failed(
        self,
        record_id: uuid.UUID,
        error: str,
    ) -> None:
        await self.db.execute(
            update(PreprocessedData)
            .where(PreprocessedData.id == record_id)
            .values(
                status=PreprocessStatus.FAILED,
                error_message=error,
                updated_at=datetime.utcnow(),
            )
        )

    async def mark_skipped_duplicate(
        self,
        record_id: uuid.UUID,
    ) -> None:
        await self.db.execute(
            update(PreprocessedData)
            .where(PreprocessedData.id == record_id)
            .values(
                status=PreprocessStatus.SKIPPED_DUP,
                updated_at=datetime.utcnow(),
            )
        )

    async def mark_rejected(
        self,
        record_id: uuid.UUID,
    ) -> None:
        await self.db.execute(
            update(PreprocessedData)
            .where(PreprocessedData.id == record_id)
            .values(
                status=PreprocessStatus.REJECTED,
                updated_at=datetime.utcnow(),
            )
        )

    # ── Delete ────────────────────────────────────────────────────────────────

    async def delete(self, record_id: uuid.UUID) -> bool:
        record = await self.get_by_id(record_id)
        if not record:
            return False
        await self.db.delete(record)
        await self.db.flush()
        return True

    async def delete_by_job_id(self, job_id: uuid.UUID) -> bool:
        """
        Called when the parent ingestion job is deleted.
        The ORM CASCADE on job_id handles this automatically,
        but this method is available for explicit use if needed.
        """
        record = await self.get_by_job_id(job_id)
        if not record:
            return False
        await self.db.delete(record)
        await self.db.flush()
        return True