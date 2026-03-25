from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ==============================================================================
# Enums
# ==============================================================================

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


# ==============================================================================
# Strategy config schemas
# ==============================================================================

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
    embedding_model:             str           = Field(
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


# ==============================================================================
# Internal dataclasses — used inside the pipeline, never exposed via API
# ==============================================================================

@dataclass
class ChunkItem:
    """Single chunk produced by a chunking strategy."""
    content:         str
    chunk_index:     int
    chunk_type:      str
    id:              str           = field(default_factory=lambda: str(uuid.uuid4()))
    parent_chunk_id: Optional[str] = None


@dataclass
class ChunkingResult:
    """
    Returned by every BaseChunker.chunk() call.
    passed=False stops the pipeline — no chunks are saved.
    """
    chunks:        list[ChunkItem]
    passed:        bool            = True
    parent_chunks: list[ChunkItem] = field(default_factory=list)
    error_message: Optional[str]   = None



# ==============================================================================
# DB schemas — used by ChunkRepository
# ==============================================================================

class ChunkCreate(BaseModel):
    """Schema for creating a new chunk in the database."""
    id:              Optional[uuid.UUID] = None  # Set explicitly for parent-child linking
    tenant_id:       uuid.UUID
    job_id:          uuid.UUID
    source_id:       uuid.UUID  # Links to ExtractedContent
    chunk_text:      str
    chunk_index:     int
    token_count:     float = 0.0
    page_number:     Optional[int] = None
    section_title:   Optional[str] = None
    heading_level:   Optional[int] = None
    language:        str = "UNKNOWN"
    lang_confidence: float = 0.0
    chunk_strategy:  str  # ChunkStrategy.value
    parent_chunk_id: Optional[uuid.UUID] = None
    topic:           Optional[str] = None
    doc_type:        Optional[str] = None
    entities:        Optional[dict] = None
    keywords:        Optional[list[str]] = None


class ChunkUpdate(BaseModel):
    """Schema for updating a chunk."""
    content:       Optional[str] = None
    status:        Optional[ChunkStatus] = None
    embedding:     Optional[list[float]] = None
    error_message: Optional[str] = None


# ==============================================================================
# API request / response schemas — used by the route
# ==============================================================================

class ChunkingRequest(BaseModel):
    """POST /api/v1/chunking - Create chunks for a job."""
    tenant_id:       uuid.UUID = Field(..., description="Tenant ID")
    job_id:          uuid.UUID = Field(..., description="Ingestion job ID")
    strategy:        ChunkStrategy = Field(
        default=ChunkStrategy.RECURSIVE,
        description="fixed | recursive | semantic | agentic | parent_child"
    )
    fixed_config:        Optional[FixedSizeConfig]   = None
    recursive_config:    Optional[RecursiveConfig]   = None
    semantic_config:     Optional[SemanticConfig]    = None
    agentic_config:      Optional[AgenticConfig]     = None
    parent_child_config: Optional[ParentChildConfig] = None


class ChunkingResponse(BaseModel):
    """Response for chunking operations."""
    job_id:           uuid.UUID
    tenant_id:        uuid.UUID
    chunks_created:   int
    chunk_strategy:   str
    message:          str
    status:           str  # completed, failed, etc.
