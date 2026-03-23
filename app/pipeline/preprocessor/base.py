from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.preprocessor import PreprocessingResult


class BasePreprocessor(ABC):
    """
    Abstract base for a single preprocessing step.

    Each step receives the current text, applies its transformation,
    and returns a PreprocessingResult.

    If passed=False the pipeline stops — no further steps run.
    This is how QualityFilterStep signals junk and DeduplicationStep
    signals a duplicate without raising an exception.

    Mirrors BaseExtractor from the ingestion pipeline.
    """

    @abstractmethod
    def process(self, text: str) -> PreprocessingResult:
        """
        Apply this step to text.

        Args:
            text: Current text after all previous steps.

        Returns:
            PreprocessingResult with:
                preprocessed_text  — text after this step (may be unchanged)
                passed             — False stops the pipeline immediately
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
