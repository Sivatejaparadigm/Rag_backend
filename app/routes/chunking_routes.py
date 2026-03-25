"""Routes for chunking operations."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
import uuid

from app.core.database import get_db
from app.schemas.chunking_schemas import ChunkingRequest, ChunkingResponse, ChunkStrategy
from app.repositories.job_repository import JobRepository
from app.repositories.preprocessor_repository import PreprocessedDataRepository
from app.repositories.chunk_repository import ChunkRepository
from app.pipeline.chunking.chunking_pipeline import ChunkingPipeline

router = APIRouter()


@router.post("/", response_model=ChunkingResponse)
async def create_chunks(
    req: ChunkingRequest,
    db: AsyncSession = Depends(get_db),
) -> ChunkingResponse:
    """
    Create chunks for a job.
    
    Minimum required:
        {
            "tenant_id": "<uuid>",
            "job_id": "<uuid>",
            "strategy": "recursive"
        }
    """
    try:
        job_repo = JobRepository(db)
        preprocess_repo = PreprocessedDataRepository(db)
        chunk_repo = ChunkRepository(db)
        
        # Load the job
        job = await job_repo.get_job(job_id=req.job_id, tenant_id=req.tenant_id)
        if not job:
            raise ValueError(f"Job not found: {req.job_id}")
        
        # Load preprocessed data for this job
        preprocessed_records = await preprocess_repo.list_by_job_id(
            job_id=req.job_id, 
            tenant_id=req.tenant_id
        )
        if not preprocessed_records:
            raise ValueError(f"No preprocessed data found for job: {req.job_id}")
        
        # Select the correct config
        config = None
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

        # Create chunking pipeline and run it
        pipeline = ChunkingPipeline(
            job_repo=job_repo,
            db=db,
            strategy=req.strategy,
            config=config,
        )
        
        result = await pipeline.run(
            job_id=req.job_id,
            tenant_id=req.tenant_id,
        )
        
        chunks_created = result.get("chunks_saved", 0) + result.get("parents_saved", 0)
        
        return ChunkingResponse(
            job_id=req.job_id,
            tenant_id=req.tenant_id,
            chunks_created=chunks_created,
            chunk_strategy=req.strategy.value if hasattr(req.strategy, 'value') else str(req.strategy),
            message=result.get("message", f"Successfully created {chunks_created} chunks"),
            status=result.get("status", "completed").value if hasattr(result.get("status", "completed"), 'value') else result.get("status", "completed"),
        )
        
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Chunking failed: {str(exc)}")


@router.get("/job/{job_id}")
async def get_job_chunks(
    job_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(...),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get chunks for a job."""
    chunk_repo = ChunkRepository(db)
    
    chunks = await chunk_repo.get_by_job_id(
        job_id=job_id,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )
    
    total = await chunk_repo.count_by_job(job_id=job_id, tenant_id=tenant_id)
    
    return {
        "job_id": job_id,
        "tenant_id": tenant_id,
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
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Delete a single chunk by its chunk ID and tenant ID."""
    chunk_repo = ChunkRepository(db)
    deleted = await chunk_repo.delete_by_id(chunk_id=chunk_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Chunk not found: {chunk_id}")
    await db.commit()
    return {
        "message": f"Chunk {chunk_id} deleted successfully",
        "chunk_id": chunk_id,
        "tenant_id": tenant_id,
    }


@router.delete("/job/{job_id}")
async def delete_chunks_by_job(
    job_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Delete all chunks for a job by job ID and tenant ID."""
    chunk_repo = ChunkRepository(db)
    deleted_count = await chunk_repo.delete_by_job_id(job_id=job_id, tenant_id=tenant_id)
    if deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"No chunks found for job: {job_id}")
    await db.commit()
    return {
        "message": f"Deleted {deleted_count} chunks for job {job_id}",
        "job_id": job_id,
        "tenant_id": tenant_id,
        "deleted_count": deleted_count,
    }
