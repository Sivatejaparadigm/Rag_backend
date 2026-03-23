from __future__ import annotations

from app.pipeline.extractors.base import BaseExtractor
from app.pipeline.extractors.registry import DEFAULT_REGISTRY, ExtractorRegistry
from app.pipeline.ingestions.base import BaseIngestion
from app.repositories.job_repository import JobRepository
from app.schemas.ingestion import DocumentType


class FileIngestion(BaseIngestion):
    """
    Default ingestion pipeline for uploaded files.

    Currently: extract -> persist extracted content -> mark job completed/failed.
    """

    def __init__(self, job_repo: JobRepository, registry: ExtractorRegistry | None = None) -> None:
        super().__init__(job_repo=job_repo)
        self.registry = registry or DEFAULT_REGISTRY

    def get_extractor(self, document_type: DocumentType) -> BaseExtractor:
        if document_type == DocumentType.UNKNOWN:
            raise ValueError("Unsupported/unknown document type.")
        return self.registry.get(document_type)

