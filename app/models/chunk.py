from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Float,
    Integer,
    String,
    Text,
    SmallInteger,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Chunk(Base):
    __tablename__ = "chunks"

    # ── Primary key ───────────────────────────────────────────────────────────
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # ── Multi-tenancy & Foreign keys ──────────────────────────────────────────
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)  # extracted_contents.id

    # ── Chunk content ────────────────────────────────────────────────────────
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=True)

    # ── Source document info ─────────────────────────────────────────────────
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    section_title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    heading_level: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)

    # ── Language & Strategy ──────────────────────────────────────────────────
    language: Mapped[str] = mapped_column(String(10), default="UNKNOWN")
    lang_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    chunk_strategy: Mapped[str] = mapped_column(String(50), nullable=False)

    # ── Relationships ────────────────────────────────────────────────────────
    parent_chunk_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("chunks.id", ondelete="CASCADE"),
        nullable=True,
    )

    # ── Phase 2 Metadata (initially NULL) ────────────────────────────────────
    topic: Mapped[str | None] = mapped_column(String(64), nullable=True)
    doc_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    entities: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # {"persons": [], "orgs": [], "dates": [], "locations": []}
    keywords: Mapped[list | None] = mapped_column(JSON, nullable=True)   # array of key phrases

    # ── Timestamps ───────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # ── Relationships ────────────────────────────────────────────────────────
    job: Mapped["IngestionJob"] = relationship("IngestionJob", back_populates="chunks")
    children: Mapped[list["Chunk"]] = relationship(
        "Chunk", back_populates="parent",
        foreign_keys=[parent_chunk_id], cascade="all, delete-orphan",
    )
    parent: Mapped["Chunk | None"] = relationship(
        "Chunk", back_populates="children",
        remote_side=[id], foreign_keys=[parent_chunk_id],
    )

    def __repr__(self) -> str:
        return (
            f"Chunk(id={self.id}, job_id={self.job_id}, "
            f"index={self.chunk_index}, "
            f"strategy={self.chunk_strategy})"
        )