from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.database import engine
from app.models.ingestion import Base
import app.models.preprocessor  # noqa: F401  ensure preprocessed_data is registered
import app.models.chunk          # noqa: F401  ensure chunks table is registered

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # ── Shutdown ──────────────────────────────────────────────
    await engine.dispose()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────
from app.routes.ingest import router as ingestion_router
from app.routes.preprocessor import router as preprocessor_router
from app.routes.chunking_routes import router as chunking_router

app.include_router(
    ingestion_router,
    prefix="/ingestion",
    tags=["Ingestion"],
)

app.include_router(
    preprocessor_router,
    prefix="/api/v1/preprocessor",
    tags=["Preprocessing"],
)

app.include_router(
    chunking_router,
    prefix="/api/v1/chunking",
    tags=["Chunking"],
)
