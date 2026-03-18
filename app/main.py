from fastapi import FastAPI
from app.routes import ingest

app = FastAPI(title="RAG Ingestion API")

app.include_router(ingest.router, prefix="/ingest", tags=["Ingestion"])