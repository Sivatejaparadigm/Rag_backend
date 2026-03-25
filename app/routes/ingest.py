from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.core.storage import ensure_uploads_dir, sanitize_filename
from app.pipeline.extractors.registry import DEFAULT_REGISTRY
from app.pipeline.ingestions.file_ingestion import FileIngestion
from app.repositories.job_repository import JobRepository
from app.schemas.ingestion import (
    DocumentType,
    IngestionJobResponse,
    IngestionStatus,
    JobListResponse,
    UploadResponse,
)

router = APIRouter()


# ── File validation helper ────────────────────────────────────

async def validate_file(file: UploadFile) -> bytes:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    ext = Path(file.filename).suffix.lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=415,
            detail=f"File type '{ext}' is not allowed. Allowed: {sorted(settings.ALLOWED_EXTENSIONS)}",
        )

    contents = await file.read()
    size_mb = len(contents) / (1024 * 1024)
    if size_mb > settings.MAX_FILE_SIZE_MB:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum is {settings.MAX_FILE_SIZE_MB}MB. Got {size_mb:.1f}MB",
        )

    return contents


# ── Upload single file ────────────────────────────────────────

@router.post("/upload", response_model=UploadResponse)
async def upload_and_ingest(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    tenant_id: uuid.UUID = Form(...),
    db: AsyncSession = Depends(get_db),
):
    contents = await validate_file(file)

    inferred_type = DEFAULT_REGISTRY.detect_document_type(file.filename)
    if inferred_type == DocumentType.UNKNOWN:
        raise HTTPException(
            status_code=415,
            detail=f"Cannot detect file type for '{file.filename}'",
        )

    job_repo = JobRepository(db)
    job = await job_repo.create_job(
        filename=file.filename,
        document_type=inferred_type.value,
        tenant_id=tenant_id,
    )

    uploads_dir = ensure_uploads_dir()
    tenant_dir = uploads_dir / str(tenant_id)
    tenant_dir.mkdir(parents=True, exist_ok=True)
    dest: Path = tenant_dir / f"{job.id}_{sanitize_filename(file.filename)}"

    try:
        dest.write_bytes(contents)
    except Exception as e:
        await job_repo.mark_failed(job_id=job.id, error=f"Failed to save file: {e}", retry_count=0)
        await db.commit()
        raise HTTPException(status_code=500, detail="Failed to save uploaded file") from e

    job.source_type = "file_upload"
    job.source_uri = str(dest)
    await db.commit()

    # Run extraction in background — client doesn't wait
    background_tasks.add_task(
        _run_ingestion_background,
        job_id=job.id,
        file_path=dest,
        tenant_id=tenant_id,
        document_type=inferred_type,
    )

    return UploadResponse(
        job_id=job.id,
        tenant_id=tenant_id,
        filename=job.filename,
        status=IngestionStatus.PENDING,
        message="File uploaded. Extraction running in background. Poll /jobs/{job_id} for status.",
    )


# ── Upload batch ──────────────────────────────────────────────

@router.post("/upload/batch", response_model=list[UploadResponse])
async def upload_batch(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
    tenant_id: uuid.UUID = Form(...),
    db: AsyncSession = Depends(get_db),
):
    if len(files) > settings.MAX_BATCH_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {settings.MAX_BATCH_FILES} files per batch. Got {len(files)}.",
        )

    job_repo = JobRepository(db)
    uploads_dir = ensure_uploads_dir()
    tenant_dir = uploads_dir / str(tenant_id)
    tenant_dir.mkdir(parents=True, exist_ok=True)

    responses = []

    for file in files:
        # validate each file — skip invalid ones instead of failing whole batch
        try:
            contents = await validate_file(file)
        except HTTPException:
            continue

        inferred_type = DEFAULT_REGISTRY.detect_document_type(file.filename)
        if inferred_type == DocumentType.UNKNOWN:
            continue

        job = await job_repo.create_job(
            filename=file.filename,
            document_type=inferred_type.value,
            tenant_id=tenant_id,
        )

        dest = tenant_dir / f"{job.id}_{sanitize_filename(file.filename)}"

        try:
            dest.write_bytes(contents)
        except Exception as e:
            await job_repo.mark_failed(job_id=job.id, error=str(e), retry_count=0)
            continue

        job.source_type = "file_upload"
        job.source_uri = str(dest)

        background_tasks.add_task(
            _run_ingestion_background,
            job_id=job.id,
            file_path=dest,
            tenant_id=tenant_id,
            document_type=inferred_type,
        )

        responses.append(UploadResponse(
            job_id=job.id,
            tenant_id=tenant_id,
            filename=job.filename,
            status=IngestionStatus.PENDING,
            message="Queued for extraction.",
        ))

    await db.commit()
    return responses


# ── Get single job ────────────────────────────────────────────

@router.get("/jobs/{job_id}", response_model=IngestionJobResponse)
async def get_job(
    job_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    job_repo = JobRepository(db)
    job = await job_repo.get_job(job_id, tenant_id=tenant_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ── List jobs ─────────────────────────────────────────────────

@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    tenant_id: uuid.UUID = Query(...),
    status: IngestionStatus | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    job_repo = JobRepository(db)
    status_value = status.value if status else None
    total = await job_repo.count_jobs(tenant_id=tenant_id, status=status_value)
    jobs = await job_repo.list_jobs(
        tenant_id=tenant_id,
        status=status_value,
        limit=limit,
        offset=offset,
    )
    return JobListResponse(total=total, limit=limit, offset=offset, jobs=jobs)


# ── Delete job ────────────────────────────────────────────────

@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    job_repo = JobRepository(db)
    job = await job_repo.get_job(job_id, tenant_id=tenant_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Delete file from disk if it exists
    if job.source_uri:
        file_path = Path(job.source_uri)
        if file_path.exists():
            file_path.unlink()

    deleted = await job_repo.delete_job(job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found")

    await db.commit()
    return {"message": f"Job {job_id} deleted successfully"}


# ── Retry failed job ──────────────────────────────────────────

@router.post("/jobs/{job_id}/retry", response_model=UploadResponse)
async def retry_job(
    job_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    job_repo = JobRepository(db)
    job = await job_repo.get_job(job_id, tenant_id=tenant_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != IngestionStatus.FAILED.value:
        raise HTTPException(
            status_code=400,
            detail=f"Only failed jobs can be retried. Current status: {job.status}",
        )

    if not job.source_uri or not Path(job.source_uri).exists():
        raise HTTPException(
            status_code=400,
            detail="Original file no longer exists on disk. Please re-upload.",
        )

    await job_repo.mark_processing(job_id)
    await db.commit()

    background_tasks.add_task(
        _run_ingestion_background,
        job_id=job.id,
        file_path=Path(job.source_uri),
        tenant_id=tenant_id,
        document_type=DocumentType(job.document_type),
    )

    return UploadResponse(
        job_id=job.id,
        tenant_id=tenant_id,
        filename=job.filename,
        status=IngestionStatus.PROCESSING,
        message="Retry started. Poll /jobs/{job_id} for status.",
    )


# ── Background task ───────────────────────────────────────────

async def _run_ingestion_background(
    job_id: uuid.UUID,
    file_path: Path,
    tenant_id: uuid.UUID,
    document_type: DocumentType,
) -> None:
    """
    Runs in background — has its own DB session separate from the request.
    """
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        try:
            job_repo = JobRepository(db)
            ingestion = FileIngestion(job_repo=job_repo)
            await ingestion.run(
                job_id=job_id,
                file_path=file_path,
                tenant_id=tenant_id,
                document_type=document_type,
            )
            await db.commit()
        except Exception:
            await db.commit()
