from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict


# ── Enums ──────────────────────────────────────────────────────

class IngestionStatus(str, Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    RETRYING   = "retrying"
    COMPLETED  = "completed"
    FAILED     = "failed"


class DocumentType(str, Enum):
    PDF     = "pdf"
    DOCX    = "docx"
    PPTX    = "pptx"
    XLSX    = "xlsx"
    TXT     = "txt"
    HTML    = "html"
    MD      = "md"
    RTF     = "rtf"
    CSV     = "csv"
    UNKNOWN = "unknown"


class SourceType(str, Enum):
    FILE_UPLOAD = "file_upload"
    S3          = "s3"
    URL         = "url"
    API_PUSH    = "api_push"


class DestinationType(str, Enum):
    MINIO    = "minio"
    POSTGRES = "postgres"


# ── Base ───────────────────────────────────────────────────────

class _OrmBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# Extracted Content

class ExtractedContentCreate(_OrmBaseModel):
    session_id: uuid.UUID
    raw_text: str | None = None
    pages: list[Any] | None = None
    tables: list[Any] | None = None
    warnings: list[Any] | None = None


class ExtractedContentResponse(_OrmBaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    session_id: uuid.UUID
    raw_text: str | None
    pages: list[Any] | None
    tables: list[Any] | None
    warnings: list[Any] | None
    created_at: datetime


# ── Ingestion Job ──────────────────────────────────────────────

class IngestionJobCreate(_OrmBaseModel):
    session_id: uuid.UUID
    filename: str
    document_type: DocumentType | None = None
    source_type: SourceType | None = None
    source_uri: str | None = None
    destination_type: DestinationType | None = None
    destination_uri: str | None = None


class IngestionJobStatusUpdate(_OrmBaseModel):
    status: IngestionStatus
    error_message: str | None = None
    word_count: int | None = None
    page_count: int | None = None
    completed_at: datetime | None = None


# ── List item — minimal, no content ───────────────────────────

class IngestionJobSummary(_OrmBaseModel):
    job_id: uuid.UUID = None
    session_id: uuid.UUID
    filename: str
    status: IngestionStatus
    word_count: int | None
    page_count: int | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @classmethod
    def from_orm_job(cls, job) -> IngestionJobSummary:
        return cls(
            job_id=job.id,
            session_id=job.session_id,
            filename=job.filename,
            status=job.status,
            word_count=job.word_count,
            page_count=job.page_count,
            created_at=job.created_at,
        )


# ── Single job GET — full detail ───────────────────────────────

class IngestionJobResponse(_OrmBaseModel):
    job_id: uuid.UUID = None
    session_id: uuid.UUID
    filename: str
    document_type: DocumentType | None
    status: IngestionStatus
    error_message: str | None
    word_count: int | None
    page_count: int | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @classmethod
    def from_orm_job(cls, job) -> IngestionJobResponse:
        return cls(
            job_id=job.id,
            session_id=job.session_id,
            filename=job.filename,
            document_type=job.document_type,
            status=job.status,
            error_message=job.error_message,
            word_count=job.word_count,
            page_count=job.page_count,
            created_at=job.created_at,
            updated_at=job.updated_at,
            completed_at=job.completed_at,
        )


# ── API Responses ──────────────────────────────────────────────

class UploadResponse(BaseModel):
    job_id: uuid.UUID
    session_id: uuid.UUID
    filename: str
    status: IngestionStatus
    message: str


class JobListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    jobs: list[IngestionJobSummary]