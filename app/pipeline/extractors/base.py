from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from pathlib import Path

from app.schemas.ingestion import DocumentType, ExtractedContentCreate


class BaseExtractor(ABC):
    """
    Extractor abstraction for turning an uploaded document into structured text.

    The return value must be `ExtractedContentCreate`, so the repository can persist it.
    """

    supported_type: DocumentType

    @abstractmethod
    async def extract(self, file_path: Path, session_id: uuid.UUID) -> ExtractedContentCreate:
        """
        Extract content from a document.

        Args:
            file_path: path to the saved file on disk
            session_id: passed through and stored on the content row

        Returns:
            ExtractedContentCreate — ready to be saved by the repository
        """
        raise NotImplementedError

    def _word_count(self, text: str) -> int:
        return len(text.split()) if text.strip() else 0

    def _truncate(self, text: str, max_chars: int = 1_000_000) -> str:
        if len(text) > max_chars:
            return text[:max_chars]
        return text

    def _ensure_file_exists(self, file_path: Path) -> None:
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))


class ExtractionError(Exception):
    """Raised when extraction fails."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.original = original