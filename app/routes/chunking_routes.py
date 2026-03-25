"""Routes for chunking operations."""
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import uuid

from app.core.database import get_db, AsyncSessionLocal
from app.schemas.chunking_schemas import ChunkingRequest, ChunkingResponse, ChunkStrategy, ChunkStatus
from app.repositories.job_repository import JobRepository
from app.repositories.preprocessor_repository import PreprocessedDataRepository
from app.repositories.chunk_repository import ChunkRepository
from app.pipeline.chunking.chunking_pipeline import ChunkingPipeline

logger = logging.getLogger(__name__)

router = APIRouter()


async def _run_chunking_in_background(
    job_id: uuid.UUID,
    session_id: uuid.UUID,
    strategy: ChunkStrategy,
    config,
):
    """Background worker — runs with its own DB session."""
    async with AsyncSessionLocal() as db:
        try:
            job_repo = JobRepository(db)
            pipeline = ChunkingPipeline(
                job_repo=job_repo,
                db=db,
                strategy=strategy,
                config=config,
            )
            result = await pipeline.run(job_id=job_id, session_id=session_id)
            logger.info(
                "Background chunking completed: job_id=%s strategy=%s result=%s",
                job_id, strategy.value, result,
            )
        except Exception:
            logger.exception(
                "Background chunking failed: job_id=%s strategy=%s",
                job_id, strategy.value,
            )


@router.post("/", response_model=ChunkingResponse)
async def create_chunks(
    req: ChunkingRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> ChunkingResponse:
    """
    Start chunking for a job (runs in background).

    Returns immediately with status=pending.
    Use GET /chunking/job/{job_id} to poll for results.
    """
    # ── Validate job and preprocessed data exist before accepting ──
    job_repo = JobRepository(db)
    preprocess_repo = PreprocessedDataRepository(db)

    job = await job_repo.get_job(job_id=req.job_id, session_id=req.session_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {req.job_id}")

    preprocessed_records = await preprocess_repo.list_by_job_id(
        job_id=req.job_id, session_id=req.session_id,
    )
    if not preprocessed_records:
        raise HTTPException(
            status_code=404,
            detail=f"No preprocessed data found for job: {req.job_id}",
        )

    # ── Select the correct config ──
    config = None
    config_dict = None
    if req.strategy == ChunkStrategy.FIXED:
        config = req.fixed_config
    elif req.strategy == ChunkStrategy.RECURSIVE:
        config = req.recursive_config
    elif req.strategy == ChunkStrategy.SEMANTIC:
        config = req.semantic_config
    elif req.strategy == ChunkStrategy.AGENTIC:
        config = req.agentic_config
    elif req.strategy == ChunkStrategy.PARENT_CHILD:
        config = req.parent_child_config

    if config is not None:
        config_dict = config.model_dump()

    # ── Fire background task ──
    background_tasks.add_task(
        _run_chunking_in_background,
        job_id=req.job_id,
        session_id=req.session_id,
        strategy=req.strategy,
        config=config,
    )

    return ChunkingResponse(
        job_id=req.job_id,
        session_id=req.session_id,
        chunk_strategy=req.strategy.value,
        config=config_dict,
        status=ChunkStatus.PENDING,
        message=f"Chunking task accepted and running in background using '{req.strategy.value}' strategy",
    )


@router.get("/job/{job_id}")
async def get_job_chunks(
    job_id: uuid.UUID,
    session_id: uuid.UUID = Query(...),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get chunks for a job."""
    chunk_repo = ChunkRepository(db)
    
    chunks = await chunk_repo.get_by_job_id(
        job_id=job_id,
        session_id=session_id,
        limit=limit,
        offset=offset,
    )
    
    total = await chunk_repo.count_by_job(job_id=job_id, session_id=session_id)
    
    return {
        "job_id": job_id,
        "session_id": session_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "chunks": [
            {
                "id": c.id,
                "chunk_index": c.chunk_index,
                "chunk_text": c.chunk_text[:100] + "..." if len(c.chunk_text) > 100 else c.chunk_text,
                "token_count": c.token_count,
                "chunk_strategy": c.chunk_strategy,
                "language": c.language,
            }
            for c in chunks
        ],
    }


@router.delete("/{chunk_id}")
async def delete_chunk_by_id(
    chunk_id: uuid.UUID,
    session_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Delete a single chunk by its chunk ID and tenant ID."""
    chunk_repo = ChunkRepository(db)
    deleted = await chunk_repo.delete_by_id(chunk_id=chunk_id, session_id=session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Chunk not found: {chunk_id}")
    await db.commit()
    return {
        "message": f"Chunk {chunk_id} deleted successfully",
        "chunk_id": chunk_id,
        "session_id": session_id,
    }


@router.delete("/job/{job_id}")
async def delete_chunks_by_job(
    job_id: uuid.UUID,
    session_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Delete all chunks for a job by job ID and tenant ID."""
    chunk_repo = ChunkRepository(db)
    deleted_count = await chunk_repo.delete_by_job_id(job_id=job_id, session_id=session_id)
    if deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"No chunks found for job: {job_id}")
    await db.commit()
    return {
        "message": f"Deleted {deleted_count} chunks for job {job_id}",
        "job_id": job_id,
        "session_id": session_id,
        "deleted_count": deleted_count,
    }
