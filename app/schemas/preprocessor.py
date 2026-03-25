from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ── Enums ─────────────────────────────────────────────────────

class PreprocessStatus(str, Enum):
    COMPLETED   = "completed"
    SKIPPED_DUP = "skipped_dup"
    REJECTED    = "rejected"
    FAILED      = "failed"


# ── Base ──────────────────────────────────────────────────────

class _OrmBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ── Create (internal — written to DB) ─────────────────────────

class PreprocessedDataCreate(_OrmBaseModel):
    tenant_id:          uuid.UUID
    job_id:             uuid.UUID
    content_id:         uuid.UUID
    filename:           str | None = None
    document_type:      str | None = None
    source_type:        str | None = None
    source_uri:         str | None = None
    preprocessed_text:  str | None = None        # ← add back
    preprocessed_pages: list[Any] | None = None  # ← add back
    language:           str = "UNKNOWN"          # ← add back
    lang_confidence:    float = 0.0              # ← add back
    status:             PreprocessStatus = PreprocessStatus.COMPLETED  # ← add back
    error_message:      str | None = None        # ← add back


# ── Update (internal — re-run preprocessing) ──────────────────

class PreprocessedDataUpdate(_OrmBaseModel):
    preprocessed_text:  str | None = None
    preprocessed_pages: list[Any] | None = None
    language:           str | None = None
    lang_confidence:    float | None = None
    status:             PreprocessStatus | None = None
    error_message:      str | None = None


# ── Pipeline result (internal — never persisted directly) ─────

class PreprocessingResult(BaseModel):
    preprocessed_text:  str
    preprocessed_pages: list[Any] | None = None
    language:           str = "UNKNOWN"
    lang_confidence:    float = Field(default=0.0, ge=0.0, le=1.0)
    word_count:         int = 0
    passed:             bool
    is_duplicate:       bool = False


# ── API Responses ─────────────────────────────────────────────

class PreprocessedDataResponse(_OrmBaseModel):
    """Single record — shown in GET /preprocess/{job_id}"""
    id:            uuid.UUID
    tenant_id:     uuid.UUID
    job_id:        uuid.UUID
    content_id:    uuid.UUID
    filename:      str | None
    document_type: str | None
    source_type:   str | None
    language:      str
    lang_confidence: float
    status:        PreprocessStatus
    error_message: str | None
    created_at:    datetime
    updated_at:    datetime


class PreprocessResponse(BaseModel):
    """Returned by POST /preprocess/{job_id}"""
    job_id:    uuid.UUID
    tenant_id: uuid.UUID
    filename:  str | None
    status:    PreprocessStatus
    message:   str


class PreprocessSummary(_OrmBaseModel):
    """Single item inside list response — minimal"""
    id:            uuid.UUID
    job_id:        uuid.UUID
    filename:      str | None
    document_type: str | None
    language:      str
    status:        PreprocessStatus
    created_at:    datetime


class PreprocessListResponse(BaseModel):
    """Returned by GET /preprocess?tenant_id=..."""
    total:   int
    limit:   int
    offset:  int
    records: list[PreprocessSummary]
