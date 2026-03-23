import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base
 
 
class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
 
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
 
    # Multi-tenancy
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
 
    # File info
    filename: Mapped[str] = mapped_column(String(500))
    document_type: Mapped[str] = mapped_column(String(50), nullable=True)
 
    # Source tracking
    source_type: Mapped[str] = mapped_column(String(50), nullable=True)   # file_upload, s3, url, api_push
    source_uri: Mapped[str] = mapped_column(String(1000), nullable=True)  # actual path or URL
 
    # Destination tracking
    destination_type: Mapped[str] = mapped_column(String(50), nullable=True)   # minio, postgres
    destination_uri: Mapped[str] = mapped_column(String(1000), nullable=True)  # bucket path or table name
 
    # Job status
    status: Mapped[str] = mapped_column(String(50), default="pending")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
 
    # Document stats
    word_count: Mapped[int] = mapped_column(Integer, nullable=True)
    page_count: Mapped[int] = mapped_column(Integer, nullable=True)
 
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
 
    content: Mapped["ExtractedContent"] = relationship(back_populates="job", uselist=False)
 
 
class ExtractedContent(Base):
    __tablename__ = "extracted_contents"
 
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
 
    # Multi-tenancy
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
 
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ingestion_jobs.id", ondelete="CASCADE"))
 
    # Limited raw text (max 1MB)
    raw_text: Mapped[str] = mapped_column(String(1_000_000), nullable=True)
 
    pages: Mapped[list] = mapped_column(JSONB, nullable=True)
    tables: Mapped[list] = mapped_column(JSONB, nullable=True)
    warnings: Mapped[list] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
 
    job: Mapped["IngestionJob"] = relationship(back_populates="content")