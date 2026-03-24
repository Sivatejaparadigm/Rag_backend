"""Chunking service for splitting preprocessed text into chunks."""
import logging
from typing import List
import uuid
from langchain_text_splitters import (
    CharacterTextSplitter,
    RecursiveCharacterTextSplitter
)
try:
    from langchain_experimental.text_splitter import SemanticChunker
except ImportError:
    SemanticChunker = None
from langchain_community.embeddings import HuggingFaceEmbeddings

from app.schemas.chunking_schemas import ChunkCreate, ChunkStrategy
from app.repositories.chunk_repository import ChunkRepository
from app.models.preprocessor import PreprocessedData

logger = logging.getLogger(__name__)


class ChunkingService:
    """Service for chunking preprocessed documents."""

    def __init__(self, chunk_repo: ChunkRepository):
        self.chunk_repo = chunk_repo
        # Initialize embeddings for semantic chunking (lazy load)
        self._embeddings = None

    @property
    def embeddings(self):
        """Lazy load embeddings for semantic chunking."""
        if self._embeddings is None:
            try:
                self._embeddings = HuggingFaceEmbeddings(
                    model_name="sentence-transformers/all-MiniLM-L6-v2"
                )
            except Exception as e:
                logger.warning(f"Failed to load embeddings: {e}, semantic chunking will be unavailable")
                self._embeddings = False
        return self._embeddings if self._embeddings else None

    async def chunk_job(
        self,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
        preprocessed_records: List[PreprocessedData],
        strategy: ChunkStrategy,
        chunk_size: int = 500,
        chunk_overlap: int = 100,
    ) -> int:
        """
        Chunk all preprocessed records for a job and store chunks.
        
        Returns the total number of chunks created.
        """
        all_chunk_creates: List[ChunkCreate] = []
        
        chunk_counter = 0

        for record in preprocessed_records:
            pages = []
            if getattr(record, "preprocessed_pages", None):
                pages = record.preprocessed_pages
            else:
                pages = [{
                    "page_number": None,
                    "text": (record.preprocessed_text or "").strip(),
                    "language": getattr(record, "language", "UNKNOWN") or "UNKNOWN",
                    "lang_confidence": getattr(record, "lang_confidence", 0.0) or 0.0,
                }]

            if not pages:
                logger.warning(f"No preprocessed pages for record {record.id}, skipping")
                continue

            for page in pages:
                page_text = (page.get("text") or "").strip()
                if not page_text:
                    logger.warning(
                        "ChunkingService: empty page text for record %s page %s - skipping",
                        record.id,
                        page.get("page_number"),
                    )
                    continue

                try:
                    page_chunks = self._apply_strategy(
                        text=page_text,
                        strategy=strategy,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                    )
                except Exception as exc:
                    logger.exception(
                        "ChunkingService: strategy failed for record %s page %s",
                        record.id,
                        page.get("page_number"),
                    )
                    continue

                page_lang = page.get("language") or getattr(record, "language", "UNKNOWN") or "UNKNOWN"
                page_lang_conf = page.get("lang_confidence") or getattr(record, "lang_confidence", 0.0) or 0.0
                page_number = page.get("page_number")

                for chunk_text in page_chunks:
                    chunk_create = ChunkCreate(
                        tenant_id=tenant_id,
                        job_id=job_id,
                        source_id=record.content_id,
                        chunk_text=chunk_text,
                        chunk_index=chunk_counter,
                        token_count=len(chunk_text.split()) * 1.3 if chunk_text else 0,
                        page_number=page_number,
                        section_title=page.get("section_title"),
                        heading_level=page.get("heading_level"),
                        language=page_lang,
                        lang_confidence=page_lang_conf,
                        chunk_strategy=strategy.value,
                        parent_chunk_id=None,
                        topic=None,
                        doc_type=record.document_type if hasattr(record, "document_type") else None,
                        entities=None,
                        keywords=None,
                    )
                    all_chunk_creates.append(chunk_create)
                    chunk_counter += 1
        
        # Bulk insert all chunks
        if all_chunk_creates:
            await self.chunk_repo.create_many(all_chunk_creates)
            logger.info(f"Created {len(all_chunk_creates)} chunks for job {job_id}")
        
        return len(all_chunk_creates)

    def _apply_strategy(
        self,
        text: str,
        strategy: ChunkStrategy,
        chunk_size: int = 500,
        chunk_overlap: int = 100,
    ) -> List[str]:
        """Apply the selected chunking strategy and return list of chunk texts."""
        
        if strategy == ChunkStrategy.RECURSIVE:
            return self._recursive_chunking(text, chunk_size, chunk_overlap)
        
        elif strategy == ChunkStrategy.FIXED:
            return self._fixed_chunking(text, chunk_size, chunk_overlap)
        
        elif strategy == ChunkStrategy.SEMANTIC:
            return self._semantic_chunking(text, chunk_size)
        
        elif strategy == ChunkStrategy.AGENTIC:
            return self._agentic_chunking(text, chunk_size, chunk_overlap)
        
        elif strategy == ChunkStrategy.PARENT_CHILD:
            return self._parent_child_chunking(text, chunk_size, chunk_overlap)
        
        else:
            logger.warning(f"Unknown strategy {strategy.value}, defaulting to recursive")
            return self._recursive_chunking(text, chunk_size, chunk_overlap)

    def _recursive_chunking(
        self,
        text: str,
        chunk_size: int,
        chunk_overlap: int,
    ) -> List[str]:
        """Recursive character chunking - preserves document structure."""
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", ". ", " ", ""],
            length_function=len,
        )
        docs = splitter.create_documents([text])
        return [doc.page_content for doc in docs]

    def _fixed_chunking(
        self,
        text: str,
        chunk_size: int,
        chunk_overlap: int,
    ) -> List[str]:
        """Fixed size chunking - chunks of exact size."""
        splitter = CharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separator=" ",
            length_function=len,
        )
        docs = splitter.create_documents([text])
        return [doc.page_content for doc in docs]

    def _semantic_chunking(
        self,
        text: str,
        chunk_size: int = 500,
    ) -> List[str]:
        """
        Semantic chunking - chunks text based on semantic similarity.
        Uses embeddings to identify natural breakpoints in content.
        """
        if self.embeddings is None or SemanticChunker is None:
            logger.warning("Semantic chunker not available, falling back to recursive chunking")
            return self._recursive_chunking(text, chunk_size, 100)

        try:
            splitter = SemanticChunker(
                embeddings=self.embeddings,
                breakpoint_threshold_type="percentile",
            )
            docs = splitter.create_documents([text])
            return [doc.page_content for doc in docs]
        except Exception as e:
            logger.warning(f"Semantic chunking failed: {e}, falling back to recursive")
            return self._recursive_chunking(text, chunk_size, 100)

    def _agentic_chunking(
        self,
        text: str,
        chunk_size: int,
        chunk_overlap: int,
    ) -> List[str]:
        """
        Agentic chunking - uses LLM-guided chunking strategy.
        For now, implements an intelligent chunking by identifying natural boundaries.
        Full agentic approach would use an LLM to determine chunk boundaries.
        """
        # Strategy: Split by sentences first, then group intelligently
        import re
        
        # Split by sentences (period followed by space, or newlines)
        sentences = re.split(r'(?<=[.!?])\s+|\n+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            test_chunk = current_chunk + " " + sentence if current_chunk else sentence
            
            # If adding this sentence doesn't exceed chunk_size, add it
            if len(test_chunk) <= chunk_size:
                current_chunk = test_chunk
            else:
                # If current chunk has content, save it
                if current_chunk:
                    chunks.append(current_chunk)
                # Start new chunk with current sentence
                current_chunk = sentence
        
        # Add any remaining content
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks if chunks else [text]

    def _parent_child_chunking(
        self,
        text: str,
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
    ) -> List[str]:
        """
        Parent-child chunking - creates two levels of chunks.
        Returns flattened list, but parent_chunk_id would be set during storage.
        Parent chunks are larger, child chunks are smaller excerpts.
        """
        # For now, return larger chunks (parent chunks)
        # In full implementation, would create parent and child relationships
        parent_chunk_size = chunk_size * 2  # Parents are 2x the size
        
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=parent_chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", ". ", " ", ""],
            length_function=len,
        )
        docs = splitter.create_documents([text])
        
        logger.info(f"Parent-child: created {len(docs)} parent chunks (child chunking deferred)")
        return [doc.page_content for doc in docs]

