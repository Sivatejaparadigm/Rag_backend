from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ── Enums ─────────────────────────────────────────────────────────────────────

class PreprocessStatus(str, Enum):
    """
    Final outcome of the full preprocessing pipeline.
    Mirrors the PreprocessStatus enum in the ORM model.
    """
    COMPLETED   = "completed"    # clean text ready for Chunking Engine
    SKIPPED_DUP = "skipped_dup"  # BLAKE3 or MinHash LSH flagged as duplicate
    REJECTED    = "rejected"     # spaCy / junk filter dropped entire document
    FAILED      = "failed"       # unhandled exception during preprocessing


# ── Base ──────────────────────────────────────────────────────────────────────

class _OrmBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ── Create ────────────────────────────────────────────────────────────────────

class PreprocessedDataCreate(_OrmBaseModel):
    """
    Written by preprocess_extracted_content() after a successful pipeline run.
    All fields map directly to columns on the preprocessed_data table.
    """

    # ── FK + tenancy ──────────────────────────────────────────────────────────
    tenant_id:  uuid.UUID
    job_id:     uuid.UUID
    content_id: uuid.UUID   # extracted_contents.id — source of raw_text

    # ── Document identity (denormalised from IngestionJob) ────────────────────
    filename:      str | None = None
    document_type: str | None = None   # e.g. "pdf", "docx"
    source_type:   str | None = None   # e.g. "file_upload", "s3", "confluence"
    source_uri:    str | None = None

    # ── Text payload ──────────────────────────────────────────────────────────
    preprocessed_text: str | None = None   # after full pipeline — feeds Chunker
    preprocessed_pages: list[Any] | None = None  # list of preprocessed page objects

    # ── Language detection ────────────────────────────────────────────────────
    language:       str = "UNKNOWN"   # detected language code
    lang_confidence: float = Field(default=0.0, ge=0.0, le=1.0)   # confidence score

    # ── Pipeline outcome ──────────────────────────────────────────────────────
    status:        PreprocessStatus = PreprocessStatus.COMPLETED
    error_message: str | None = None   # populated only when status = FAILED


# ── Update ────────────────────────────────────────────────────────────────────

class PreprocessedDataUpdate(_OrmBaseModel):
    """
    Used when re-running preprocessing on an already-stored record
    (e.g. tenant changes PII settings and triggers a re-process).
    All fields optional — only set what changed.
    """
    preprocessed_text: str | None = None
    preprocessed_pages: list[Any] | None = None
    language:          str | None = None
    lang_confidence:   float | None = None
    status:            PreprocessStatus | None = None
    error_message:     str | None = None


# ── Response ──────────────────────────────────────────────────────────────────

class PreprocessedDataResponse(_OrmBaseModel):
    """
    Returned by the API and repository get methods.
    Mirrors every column on the preprocessed_data table.
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    id:         uuid.UUID
    tenant_id:  uuid.UUID
    job_id:     uuid.UUID
    content_id: uuid.UUID

    # ── Document identity ─────────────────────────────────────────────────────
    filename:      str | None
    document_type: str | None
    source_type:   str | None
    source_uri:    str | None

    # ── Text ──────────────────────────────────────────────────────────────────
    preprocessed_text: str | None
    preprocessed_pages: list[Any] | None

    # ── Language detection ────────────────────────────────────────────────────
    language:        str
    lang_confidence: float

    # ── Outcome ───────────────────────────────────────────────────────────────
    status:        PreprocessStatus
    error_message: str | None

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: datetime
    updated_at: datetime


# ── Pipeline result (internal — not persisted) ────────────────────────────────

class PreprocessingResult(BaseModel):
    """
    Internal dataclass returned by apply_preprocessing().
    Never written to the DB directly — the service layer maps it to
    PreprocessedDataCreate before persisting.

    Maps to the dict returned by apply_preprocessing() in the service file:
        {
            "preprocessed_text": str,
            "language":          str,
            "lang_confidence":   float,
            "word_count":        int,
            "passed":            bool,
        }
    """
    preprocessed_text: str
    preprocessed_pages: list[Any] | None = None
    language:          str = "UNKNOWN"
    lang_confidence:   float = Field(default=0.0, ge=0.0, le=1.0)
    word_count:        int = 0
    passed:            bool   # False → status will be REJECTED
    # When DeduplicationStep rejects, we use this to distinguish duplicates
    # from "rejected junk" so the pipeline can set SKIPPED_DUP vs REJECTED.
    is_duplicate:      bool = False


# ── API Responses ─────────────────────────────────────────────────────────────

class PreprocessResponse(BaseModel):
    """
    Returned by POST /preprocess/{job_id}
    Mirrors UploadResponse pattern from chunk_schemas.
    """
    job_id:     uuid.UUID
    tenant_id:  uuid.UUID
    filename:   str | None
    status:     PreprocessStatus
    message:    str
    preprocessed_text:  str | None = None
    preprocessed_pages: list[Any] | None = None


class PreprocessListResponse(BaseModel):
    """
    Returned by GET /preprocess?tenant_id=...
    """
    total:   int
    limit:   int
    offset:  int
    records: list[PreprocessedDataResponse]