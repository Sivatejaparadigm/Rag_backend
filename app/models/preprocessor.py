import uuid
from datetime import datetime
from sqlalchemy import (
    String, Text, DateTime,
    ForeignKey, Enum
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base
import enum


# ── Enum ─────────────────────────────────────────────────────────────────────

class PreprocessStatus(str, enum.Enum):
    """
    completed   → pipeline ran successfully; preprocessed_text ready for Chunking Engine.
    skipped_dup → duplicate detected by BLAKE3 or MinHash LSH; no further processing.
    rejected    → spaCy quality filter determined text is structural junk.
    failed      → unhandled exception during preprocessing.
    """
    completed   = "completed"
    skipped_dup = "skipped_dup"
    rejected    = "rejected"
    failed      = "failed"


# ── Model ────────────────────────────────────────────────────────────────────

class PreprocessedData(Base):
    """
    Stores the before/after text for every document that passes through
    the Preprocessor stage, along with the pipeline outcome and key
    identity fields from the parent tables.

    Pipeline this record represents:
        ExtractedContent.raw_text
            → ftfy            (encoding fix)
            → NFKC            (unicode normalisation)
            → regex           (whitespace cleanup)
            → spaCy           (quality filter)
            → fastText/lingua (language detection)
            → Presidio+GLiNER (PII redaction, optional)
            → BLAKE3+MinHash  (deduplication)
            → preprocessed_text

    Foreign keys
    ────────────
    job_id     → ingestion_jobs.id       source job, file info, tenant
    content_id → extracted_contents.id   raw text before preprocessing

    Denormalised columns (from IngestionJob)
    ────────────────────────────────────────
    tenant_id, filename, document_type, source_type, source_uri are copied
    so this table is independently queryable without a join.
    """

    __tablename__ = "preprocessed_data"

    # ── Primary key ───────────────────────────────────────────────────────────

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    # ── Multi-tenancy ─────────────────────────────────────────────────────────

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
        comment="Copied from ingestion_jobs.tenant_id.",
    )

    # ── Foreign keys ──────────────────────────────────────────────────────────

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    content_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("extracted_contents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ── Document identity (denormalised from IngestionJob) ────────────────────

    filename: Mapped[str] = mapped_column(
        String(500),
        nullable=True,
        comment="Copied from ingestion_jobs.filename.",
    )

    document_type: Mapped[str] = mapped_column(
        String(50),
        nullable=True,
        comment="Copied from ingestion_jobs.document_type.",
    )

    source_type: Mapped[str] = mapped_column(
        String(50),
        nullable=True,
        comment="Copied from ingestion_jobs.source_type. "
                "e.g. file_upload, s3, confluence, url, git.",
    )

    source_uri: Mapped[str] = mapped_column(
        String(1000),
        nullable=True,
        comment="Copied from ingestion_jobs.source_uri.",
    )

    # ── Text payload ──────────────────────────────────────────────────────────

    raw_text: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment="Unmodified text from the Document Parser "
                "(Docling / unstructured.io). "
                "Mirrors extracted_contents.raw_text.",
    )

    preprocessed_text: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment="Clean text after the full preprocessing pipeline. "
                "This field feeds the Chunking Engine.",
    )

    # ── Pipeline outcome ──────────────────────────────────────────────────────

    status: Mapped[PreprocessStatus] = mapped_column(
        Enum(PreprocessStatus),
        default=PreprocessStatus.completed,
        index=True,
        comment="completed | skipped_dup | rejected | failed",
    )

    error_message: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment="Exception detail when status = 'failed'. Null otherwise.",
    )

    # ── Timestamps ────────────────────────────────────────────────────────────

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    # ── Relationships ─────────────────────────────────────────────────────────

    job: Mapped["IngestionJob"] = relationship(
        "IngestionJob",
        foreign_keys=[job_id],
        back_populates=None,
        lazy="select",
    )

    content: Mapped["ExtractedContent"] = relationship(
        "ExtractedContent",
        foreign_keys=[content_id],
        back_populates=None,
        lazy="select",
    )