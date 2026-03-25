from __future__ import annotations

import logging
import os
import uuid
from langchain_text_splitters import CharacterTextSplitter, RecursiveCharacterTextSplitter
from langchain_experimental.text_splitter import SemanticChunker as LangchainSemanticChunker
from langchain_community.embeddings import HuggingFaceEmbeddings

from app.pipeline.chunking.base import BaseChunker
from app.schemas.chunking_schemas import (
    ChunkingResult,
    ChunkItem,
    FixedSizeConfig,
    RecursiveConfig,
    SemanticConfig,
    AgenticConfig,
    ParentChildConfig,
)

logger = logging.getLogger(__name__)


# ── Strategy 1 — Fixed-size with Overlap ──────────────────────────────────────

class FixedSizeChunker(BaseChunker):
    """
    Splits text into equal-sized character chunks with optional overlap.

    Simple and fast — good for homogeneous documents (plain logs, CSV rows).
    May cut sentences mid-way; use RecursiveChunker when sentence boundaries
    matter for retrieval quality.

    Config:
        chunk_size    — maximum characters per chunk (default 500)
        chunk_overlap — characters shared between adjacent chunks (default 100)
        separator     — split boundary, tried before hard-cutting (default \\n)
    """

    def __init__(self, config: FixedSizeConfig | None = None) -> None:
        self.config = config or FixedSizeConfig()

    def chunk(self, text: str) -> ChunkingResult:
        try:
            splitter = CharacterTextSplitter(
                chunk_size    = self.config.chunk_size,
                chunk_overlap = self.config.chunk_overlap,
                separator     = self.config.separator,
            )
            docs   = splitter.create_documents([text])
            chunks = [
                ChunkItem(
                    content     = doc.page_content,
                    chunk_index = i,
                    chunk_type  = "fixed",
                )
                for i, doc in enumerate(docs)
            ]
            logger.debug("FixedSizeChunker: produced %d chunks", len(chunks))
            return ChunkingResult(chunks=chunks, passed=True)

        except Exception as exc:
            logger.exception("FixedSizeChunker failed")
            return ChunkingResult(chunks=[], passed=False, error_message=str(exc))


# ── Strategy 2 — Recursive / Hierarchical ─────────────────────────────────────

class RecursiveChunker(BaseChunker):
    """
    Splits text using a priority-ordered list of separators.

    Tries each separator in order (paragraph → sentence → word → character)
    and falls back to the next only if the chunk is still too large.
    Preserves paragraph and sentence boundaries better than FixedSizeChunker.

    This is the recommended default for most document types.

    Config:
        chunk_size    — maximum characters per chunk (default 500)
        chunk_overlap — characters shared between adjacent chunks (default 100)
        separators    — ordered list tried in sequence
                        (default [\\n\\n, \\n, ". ", " ", ""])
    """

    def __init__(self, config: RecursiveConfig | None = None) -> None:
        self.config = config or RecursiveConfig()

    def chunk(self, text: str) -> ChunkingResult:
        try:
            splitter = RecursiveCharacterTextSplitter(
                chunk_size    = self.config.chunk_size,
                chunk_overlap = self.config.chunk_overlap,
                separators    = self.config.separators,
            )
            docs   = splitter.create_documents([text])
            chunks = [
                ChunkItem(
                    content     = doc.page_content,
                    chunk_index = i,
                    chunk_type  = "recursive",
                )
                for i, doc in enumerate(docs)
            ]
            logger.debug("RecursiveChunker: produced %d chunks", len(chunks))
            return ChunkingResult(chunks=chunks, passed=True)

        except Exception as exc:
            logger.exception("RecursiveChunker failed")
            return ChunkingResult(chunks=[], passed=False, error_message=str(exc))


# ── Strategy 3 — Semantic Chunking ────────────────────────────────────────────

class SemanticChunker(BaseChunker):
    """
    Detects topic shifts using sentence embedding cosine similarity.

    Embeds consecutive sentences and starts a new chunk when the similarity
    drops below the breakpoint threshold. Produces topically coherent chunks
    at the cost of variable chunk sizes.

    Best for: dense technical documents, research papers, multi-topic reports.
    Avoid for: very short documents (< 200 words) — not enough signal.

    The embedding model is loaded once per instance. Use a shared instance
    across requests to avoid repeated model loading.

    Config:
        embedding_model              — HuggingFace model name
        breakpoint_threshold_type    — percentile | standard_deviation |
                                       interquartile | gradient
        breakpoint_threshold_amount  — sensitivity (higher = fewer, larger chunks)
    """

    def __init__(self, config: SemanticConfig | None = None) -> None:
        self.config = config or SemanticConfig()
        logger.debug(
            "SemanticChunker: loading embedding model %s",
            self.config.embedding_model,
        )
        self._embeddings = HuggingFaceEmbeddings(
            model_name=self.config.embedding_model
        )

    def chunk(self, text: str) -> ChunkingResult:
        try:
            splitter = LangchainSemanticChunker(
                embeddings                   = self._embeddings,
                breakpoint_threshold_type    = self.config.breakpoint_threshold_type,
                breakpoint_threshold_amount  = self.config.breakpoint_threshold_amount,
            )
            docs   = splitter.create_documents([text])
            chunks = [
                ChunkItem(
                    content     = doc.page_content,
                    chunk_index = i,
                    chunk_type  = "semantic",
                )
                for i, doc in enumerate(docs)
            ]
            logger.debug("SemanticChunker: produced %d chunks", len(chunks))
            return ChunkingResult(chunks=chunks, passed=True)

        except Exception as exc:
            logger.exception("SemanticChunker failed")
            return ChunkingResult(chunks=[], passed=False, error_message=str(exc))


# ── Strategy 4 — Agentic (LLM-driven) ────────────────────────────────────────

class AgenticChunker(BaseChunker):
    """
    Uses an LLM to identify natural topic boundaries in the text.

    The LLM reads the text and returns chunks separated by ---CHUNK--- markers.
    Produces the most semantically intelligent splits at the cost of latency
    and API usage.

    Best for: complex multi-topic documents where retrieval precision matters
    most (contracts, research papers, mixed-format reports).
    Avoid for: high-throughput pipelines — use RecursiveChunker there.

    Config:
        model       — LLM model identifier (default gemini-2.5-flash)
        temperature — 0.0 for deterministic splits (recommended)
        max_length  — max input chars sent to LLM per call (default 2000)
        provider    — google | openai | anthropic
    """

    _SEPARATOR = "---CHUNK---"

    _PROMPT = """You are a document chunking expert.
Split the following text into meaningful chunks based on topic and context.

Rules:
- Each chunk must be self-contained and focused on ONE topic
- Keep related sentences together
- Do NOT summarise or modify the text
- Separate each chunk with exactly: ---CHUNK---

TEXT:
{text}

Return ONLY the chunks separated by ---CHUNK--- and nothing else."""

    def __init__(self, config: AgenticConfig | None = None) -> None:
        self.config = config or AgenticConfig()

    def _build_llm(self):
        if self.config.provider == "google":
            from langchain_google_genai import ChatGoogleGenerativeAI
            from app.core.config import settings
            return ChatGoogleGenerativeAI(
                model          = self.config.model,
                google_api_key = settings.GOOGLE_API_KEY,
                temperature    = self.config.temperature,
            )
        if self.config.provider == "openai":
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model       = self.config.model,
                temperature = self.config.temperature,
            )
        if self.config.provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model       = self.config.model,
                temperature = self.config.temperature,
            )
        raise ValueError(f"Unsupported provider: {self.config.provider}")

    def chunk(self, text: str) -> ChunkingResult:
        try:
            llm      = self._build_llm()
            prompt   = self._PROMPT.format(text=text[: self.config.max_length])
            response = llm.invoke(prompt)

            raw_chunks = [
                c.strip()
                for c in response.content.split(self._SEPARATOR)
                if c.strip()
            ]

            chunks = [
                ChunkItem(
                    content     = c,
                    chunk_index = i,
                    chunk_type  = "agentic",
                )
                for i, c in enumerate(raw_chunks)
            ]

            logger.debug("AgenticChunker: produced %d chunks", len(chunks))
            return ChunkingResult(chunks=chunks, passed=True)

        except Exception as exc:
            logger.exception("AgenticChunker failed")
            return ChunkingResult(chunks=[], passed=False, error_message=str(exc))


# ── Strategy 5 — Parent-Child Linking ─────────────────────────────────────────

class ParentChildChunker(BaseChunker):
    """
    Two-pass chunking designed specifically for RAG retrieval.

    Pass 1 — large parent chunks (default 1000 chars) are stored for context.
    Pass 2 — each parent is split into small child chunks (default 200 chars)
             that are indexed in the vector store.

    At query time:
        1. The vector store retrieves the most relevant child chunk.
        2. The parent chunk is returned to the LLM — giving full context
           without bloating the vector index with large chunks.

    Each child carries parent_chunk_id so the service can resolve
    the parent at retrieval time.

    Config:
        parent_chunk_size    — characters per parent chunk (default 1000)
        parent_chunk_overlap — parent overlap (default 100)
        child_chunk_size     — characters per child chunk (default 200)
        child_chunk_overlap  — child overlap (default 20)
    """

    def __init__(self, config: ParentChildConfig | None = None) -> None:
        self.config = config or ParentChildConfig()

    def chunk(self, text: str) -> ChunkingResult:
        try:
            parent_splitter = RecursiveCharacterTextSplitter(
                chunk_size    = self.config.parent_chunk_size,
                chunk_overlap = self.config.parent_chunk_overlap,
            )
            child_splitter = RecursiveCharacterTextSplitter(
                chunk_size    = self.config.child_chunk_size,
                chunk_overlap = self.config.child_chunk_overlap,
            )

            parent_docs = parent_splitter.create_documents([text])

            parent_chunks: list[ChunkItem] = []
            child_chunks:  list[ChunkItem] = []

            for p_idx, p_doc in enumerate(parent_docs):
                parent_id = str(uuid.uuid4())

                parent_chunks.append(
                    ChunkItem(
                        id          = parent_id,
                        content     = p_doc.page_content,
                        chunk_index = p_idx,
                        chunk_type  = "parent",
                    )
                )

                c_docs = child_splitter.create_documents([p_doc.page_content])
                for c_idx, c_doc in enumerate(c_docs):
                    child_chunks.append(
                        ChunkItem(
                            content         = c_doc.page_content,
                            chunk_index     = c_idx,
                            chunk_type      = "child",
                            parent_chunk_id = parent_id,
                        )
                    )

            logger.debug(
                "ParentChildChunker: %d parents, %d children",
                len(parent_chunks),
                len(child_chunks),
            )

            return ChunkingResult(
                chunks         = child_chunks,   # children go to vector store
                parent_chunks  = parent_chunks,  # parents stored for context
                passed         = True,
            )

        except Exception as exc:
            logger.exception("ParentChildChunker failed")
            return ChunkingResult(chunks=[], passed=False, error_message=str(exc))
