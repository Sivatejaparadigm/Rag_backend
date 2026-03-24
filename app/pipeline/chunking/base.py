from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.chunking_schemas import ChunkingResult


class BaseChunker(ABC):
    """
    Abstract base for a single chunking strategy.

    Each strategy receives the preprocessed text and chunking config,
    applies its splitting logic, and returns a ChunkingResult.

    If passed=False the pipeline stops immediately — no chunks are saved.
    This mirrors BasePreprocessor from the preprocessing pipeline.

    Strategies:
        FixedSizeChunker       — CharacterTextSplitter with fixed size + overlap
        RecursiveChunker       — RecursiveCharacterTextSplitter (default)
        SemanticChunker        — SemanticChunker via embedding model
        AgenticChunker         — LLM-driven topic-boundary splitting
        ParentChildChunker     — Two-pass: large parent + small child chunks
    """

    @abstractmethod
    def chunk(self, text: str) -> ChunkingResult:
        """
        Apply this chunking strategy to text.

        Args:
            text: Preprocessed text ready for chunking.

        Returns:
            ChunkingResult with:
                chunks         — list of chunk strings
                passed         — False stops the pipeline immediately
                error_message  — set on failure
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
