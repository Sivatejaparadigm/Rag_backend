from __future__ import annotations

from pathlib import Path
from typing import Final

from app.pipeline.extractors.base import BaseExtractor
from app.pipeline.extractors.extractors import (
    PDFExtractor,
    DocxExtractor,
    TextExtractor,
    CSVExtractor,
    HTMLExtractor,
    MarkdownExtractor,
    PPTXExtractor,
    XLSXExtractor,
    RTFExtractor,
)
from app.schemas.ingestion import DocumentType


class ExtractorRegistry:
    def __init__(self) -> None:
        self._extractors: dict[DocumentType, BaseExtractor] = {
            DocumentType.PDF:  PDFExtractor(),
            DocumentType.DOCX: DocxExtractor(),
            DocumentType.TXT:  TextExtractor(),
            DocumentType.CSV:  CSVExtractor(),
            DocumentType.HTML: HTMLExtractor(),
            DocumentType.MD:   MarkdownExtractor(),
            DocumentType.PPTX: PPTXExtractor(),
            DocumentType.XLSX: XLSXExtractor(),
            DocumentType.RTF:  RTFExtractor(),
        }

    def get(self, document_type: DocumentType) -> BaseExtractor:
        try:
            return self._extractors[document_type]
        except KeyError as e:
            raise ValueError(f"No extractor registered for: {document_type}") from e

    @staticmethod
    def detect_document_type(filename: str) -> DocumentType:
        mapping = {
            "pdf":  DocumentType.PDF,
            "docx": DocumentType.DOCX,
            "doc":  DocumentType.DOCX,
            "pptx": DocumentType.PPTX,
            "xlsx": DocumentType.XLSX,
            "xls":  DocumentType.XLSX,
            "txt":  DocumentType.TXT,
            "text": DocumentType.TXT,
            "html": DocumentType.HTML,
            "htm":  DocumentType.HTML,
            "md":   DocumentType.MD,
            "rtf":  DocumentType.RTF,
            "csv":  DocumentType.CSV,
        }
        suffix = Path(filename).suffix.lower().lstrip(".")
        return mapping.get(suffix, DocumentType.UNKNOWN)


DEFAULT_REGISTRY: Final = ExtractorRegistry()