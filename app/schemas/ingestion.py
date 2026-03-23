from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict


# Enums

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


# Base Model

class _OrmBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# Extracted Content

class ExtractedContentCreate(_OrmBaseModel):
    tenant_id: uuid.UUID
    raw_text: str | None = None
    pages: list[Any] | None = None
    tables: list[Any] | None = None
    warnings: list[Any] | None = None


class ExtractedContentResponse(ExtractedContentCreate):
    id: uuid.UUID
    job_id: uuid.UUID
    created_at: datetime


# Ingestion Job

class IngestionJobCreate(_OrmBaseModel):
    tenant_id: uuid.UUID
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


class IngestionJobResponse(_OrmBaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    filename: str
    document_type: DocumentType | None
    source_type: SourceType | None
    source_uri: str | None
    destination_type: DestinationType | None
    destination_uri: str | None
    status: IngestionStatus
    retry_count: int
    error_message: str | None
    word_count: int | None
    page_count: int | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    content: ExtractedContentResponse | None = None


# API Responses

class UploadResponse(BaseModel):
    job_id: uuid.UUID
    filename: str
    status: IngestionStatus
    message: str


class JobListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    jobs: list[IngestionJobResponse]