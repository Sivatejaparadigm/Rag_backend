from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Enums ─────────────────────────────────────────────────────

class ChunkStrategy(str, Enum):
    FIXED        = "fixed"
    RECURSIVE    = "recursive"
    SEMANTIC     = "semantic"
    AGENTIC      = "agentic"
    PARENT_CHILD = "parent_child"


class ChunkStatus(str, Enum):
    PENDING     = "pending"
    CHUNKED     = "chunked"
    EMBEDDED    = "embedded"
    FAILED      = "failed"
    SKIPPED_DUP = "skipped_dup"


class BreakpointType(str, Enum):
    PERCENTILE    = "percentile"
    STANDARD_DEV  = "standard_deviation"
    INTERQUARTILE = "interquartile"
    GRADIENT      = "gradient"


# ── Strategy configs (internal — passed in request body) ──────

class FixedSizeConfig(BaseModel):
    chunk_size:    int = Field(default=500, ge=50,  le=8000)
    chunk_overlap: int = Field(default=100, ge=0,   le=2000)
    separator:     str = Field(default="\n")

    @field_validator("chunk_overlap")
    @classmethod
    def overlap_less_than_size(cls, v, info):
        if "chunk_size" in info.data and v >= info.data["chunk_size"]:
            raise ValueError("chunk_overlap must be less than chunk_size")
        return v


class RecursiveConfig(BaseModel):
    chunk_size:    int       = Field(default=500, ge=50, le=8000)
    chunk_overlap: int       = Field(default=100, ge=0,  le=2000)
    separators:    list[str] = Field(default=["\n\n", "\n", ". ", " ", ""])

    @field_validator("chunk_overlap")
    @classmethod
    def overlap_less_than_size(cls, v, info):
        if "chunk_size" in info.data and v >= info.data["chunk_size"]:
            raise ValueError("chunk_overlap must be less than chunk_size")
        return v


class SemanticConfig(BaseModel):
    embedding_model:             str            = Field(
        default="sentence-transformers/all-MiniLM-L6-v2"
    )
    breakpoint_threshold_type:   BreakpointType = Field(
        default=BreakpointType.PERCENTILE
    )
    breakpoint_threshold_amount: float          = Field(
        default=90.0, ge=0.0, le=100.0
    )


class AgenticConfig(BaseModel):
    model:       str   = Field(default="gemini-2.5-flash")
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    max_length:  int   = Field(default=2000, ge=100, le=10000)
    provider:    str   = Field(default="google")


class ParentChildConfig(BaseModel):
    parent_chunk_size:    int = Field(default=1000, ge=200, le=8000)
    parent_chunk_overlap: int = Field(default=100,  ge=0,   le=1000)
    child_chunk_size:     int = Field(default=200,  ge=50,  le=2000)
    child_chunk_overlap:  int = Field(default=20,   ge=0,   le=500)

    @model_validator(mode="after")
    def child_smaller_than_parent(self):
        if self.child_chunk_size >= self.parent_chunk_size:
            raise ValueError("child_chunk_size must be smaller than parent_chunk_size")
        return self


# ── Internal dataclasses (pipeline only — never in API) ───────

@dataclass
class ChunkItem:
    content:         str
    chunk_index:     int
    chunk_type:      str
    id:              str           = field(default_factory=lambda: str(uuid.uuid4()))
    parent_chunk_id: Optional[str] = None


@dataclass
class ChunkingResult:
    chunks:        list[ChunkItem]
    passed:        bool            = True
    parent_chunks: list[ChunkItem] = field(default_factory=list)
    error_message: Optional[str]   = None


# ── DB schemas (used by ChunkRepository — never in API) ───────

class ChunkCreate(BaseModel):
    id:              Optional[uuid.UUID] = None
    session_id:       uuid.UUID
    job_id:          uuid.UUID
    source_id:       uuid.UUID
    chunk_text:      str
    chunk_index:     int
    token_count:     float = 0.0
    page_number:     Optional[int] = None
    section_title:   Optional[str] = None
    heading_level:   Optional[int] = None
    language:        str = "UNKNOWN"
    lang_confidence: float = 0.0
    chunk_strategy:  str
    parent_chunk_id: Optional[uuid.UUID] = None
    topic:           Optional[str] = None
    doc_type:        Optional[str] = None
    entities:        Optional[dict] = None
    keywords:        Optional[list[str]] = None


class ChunkUpdate(BaseModel):
    content:       Optional[str] = None
    status:        Optional[ChunkStatus] = None
    embedding:     Optional[list[float]] = None
    error_message: Optional[str] = None


# ── API request / response ─────────────────────────────────────

class ChunkingRequest(BaseModel):
    session_id:           uuid.UUID
    job_id:              uuid.UUID
    strategy:            ChunkStrategy = Field(default=ChunkStrategy.RECURSIVE)
    fixed_config:        Optional[FixedSizeConfig]   = None
    recursive_config:    Optional[RecursiveConfig]   = None
    semantic_config:     Optional[SemanticConfig]    = None
    agentic_config:      Optional[AgenticConfig]     = None
    parent_child_config: Optional[ParentChildConfig] = None


class ChunkingResponse(BaseModel):
    job_id:         uuid.UUID
    session_id:      uuid.UUID
    # chunks_created: Optional[int] = None
    chunk_strategy: str
    config:         Optional[dict] = None
    status:         ChunkStatus
    message:        str


class ChunkSummary(BaseModel):
    """Single item in list response — minimal"""
    id:             uuid.UUID
    job_id:         uuid.UUID
    chunk_index:    int
    chunk_strategy: str
    token_count:    float
    language:       str
    status:         ChunkStatus
    created_at:     datetime


class ChunkListResponse(BaseModel):
    """Returned by GET /chunks?session_id=..."""
    total:  int
    limit:  int
    offset: int
    chunks: list[ChunkSummary]
