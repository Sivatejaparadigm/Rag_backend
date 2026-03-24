from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.pipeline.preprocessor.preprocessing_pipeline import PreprocessingPipeline
from app.repositories.job_repository import JobRepository
from app.repositories.preprocessor_repository import PreprocessedDataRepository
from app.schemas.preprocessor import (
    PreprocessListResponse,
    PreprocessResponse,
    PreprocessStatus,
)

router = APIRouter()


@router.post("/preprocess/{job_id}", response_model=PreprocessResponse)
async def run_preprocess(
    job_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Run preprocessing pipeline for a specific ingestion job.

    This loads `extracted_contents` via the job relationship and persists
    the final output into `preprocessed_data`.
    """
    try:
        job_repo = JobRepository(db)
        pipeline = PreprocessingPipeline(job_repo=job_repo, db=db)
        await pipeline.run(job_id=job_id, tenant_id=tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Re-fetch persisted record so we can return raw + cleaned text even
    # when the pipeline returned `record=None` (e.g. exceptions).
    repo = PreprocessedDataRepository(db)
    record = await repo.get_by_job_id(job_id=job_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=404, detail="preprocessed_data not found for job_id")

    status: PreprocessStatus = record.status
    message = f"Preprocessing {status.value}"

    return PreprocessResponse(
        job_id=job_id,
        tenant_id=tenant_id,
        filename=record.filename,
        status=status,
        message=message,
        preprocessed_text=record.preprocessed_text,
        preprocessed_pages=record.preprocessed_pages,
    )


@router.get("/preprocess", response_model=PreprocessListResponse)
async def list_preprocessed(
    tenant_id: uuid.UUID = Query(...),
    status: PreprocessStatus | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    repo = PreprocessedDataRepository(db)
    total = await repo.count_by_tenant(tenant_id=tenant_id, status=status)
    records = await repo.list_by_tenant(tenant_id=tenant_id, status=status, limit=limit, offset=offset)
    return PreprocessListResponse(total=total, limit=limit, offset=offset, records=records)

