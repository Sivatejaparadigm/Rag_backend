from __future__ import annotations

import re
import unicodedata
import hashlib
import logging

from ftfy import fix_text
from datasketch import MinHash, MinHashLSH
from lingua import LanguageDetectorBuilder

from app.pipeline.preprocessor.base import BasePreprocessor
from app.schemas.preprocessor import PreprocessingResult

logger = logging.getLogger(__name__)


# ── Step 1 — Encoding fix ─────────────────────────────────────────────────────

class EncodingFixStep(BasePreprocessor):
    """
    Fixes mojibake, broken curly quotes, and broken accents via ftfy.
    Must run before NormalisationStep — NFKC transforms the byte patterns
    that ftfy uses to detect encoding corruption.
    """

    def process(self, text: str) -> PreprocessingResult:
        fixed = fix_text(text)
        return PreprocessingResult(
            preprocessed_text=fixed,
            passed=True,
        )


# ── Step 2 — Unicode normalisation ───────────────────────────────────────────

class NormalisationStep(BasePreprocessor):
    """
    Applies NFKC normalisation + strips Unicode control characters.

    NFKC converts:
        fullwidth Latin  ａｂｃ  → abc
        ligatures        ﬁ      → fi
        superscripts     ²      → 2

    Control characters (category 'C') are stripped after normalisation —
    these appear as PDF extraction artifacts (null bytes, soft hyphens).
    """

    def process(self, text: str) -> PreprocessingResult:
        normalised = unicodedata.normalize("NFKC", text)
        cleaned    = "".join(
            ch for ch in normalised
            if not unicodedata.category(ch).startswith("C")
        )
        return PreprocessingResult(
            preprocessed_text=cleaned,
            passed=True,
        )


# ── Step 3 — Whitespace cleanup ───────────────────────────────────────────────

class WhitespaceStep(BasePreprocessor):
    """
    Removes invisible zero-width characters, collapses multiple spaces/tabs,
    and limits consecutive newlines to 2.

    Newlines are preserved (not collapsed to spaces) because they encode
    paragraph boundaries used by the Chunking Engine downstream.

    Patterns compiled once at class level — not per call.
    """

    _ZERO_WIDTH    = re.compile(r"[\u200b\u200c\u200d\ufeff]")
    _MULTI_SPACE   = re.compile(r"[ \t]+")
    _MULTI_NEWLINE = re.compile(r"\n{3,}")

    def process(self, text: str) -> PreprocessingResult:
        text = self._ZERO_WIDTH.sub("", text)
        text = self._MULTI_SPACE.sub(" ", text)
        text = self._MULTI_NEWLINE.sub("\n\n", text)
        text = text.strip()
        return PreprocessingResult(
            preprocessed_text=text,
            passed=True,
        )


# ── Step 4 — Quality filter ───────────────────────────────────────────────────

class QualityFilterStep(BasePreprocessor):
    """
    Drops structurally junk text without using stopword removal or
    lemmatization (which harm embedding quality).

    Rejection criteria (any one fails → passed=False):
        - Fewer than MIN_WORDS words
        - More than MAX_SYMBOL_RATIO non-alphanumeric, non-space characters
        - More than MAX_DIGIT_RATIO digit characters (table rows, page numbers)

    Note: spaCy sentencizer can be added here as an optional upgrade for
    sentence-boundary-based filtering. The current implementation is
    dependency-free and fast.
    """

    MIN_WORDS        = 5
    MAX_SYMBOL_RATIO = 0.4
    MAX_DIGIT_RATIO  = 0.6

    def process(self, text: str) -> PreprocessingResult:
        if not text or not text.strip():
            return PreprocessingResult(preprocessed_text=text, passed=False)

        words  = text.split()
        total  = len(text)

        if len(words) < self.MIN_WORDS:
            logger.debug("QualityFilter: rejected (too few words: %d)", len(words))
            return PreprocessingResult(preprocessed_text=text, passed=False)

        symbol_ratio = sum(
            1 for c in text if not c.isalnum() and not c.isspace()
        ) / total

        if symbol_ratio > self.MAX_SYMBOL_RATIO:
            logger.debug("QualityFilter: rejected (symbol ratio: %.2f)", symbol_ratio)
            return PreprocessingResult(preprocessed_text=text, passed=False)

        digit_ratio = sum(1 for c in text if c.isdigit()) / total

        if digit_ratio > self.MAX_DIGIT_RATIO:
            logger.debug("QualityFilter: rejected (digit ratio: %.2f)", digit_ratio)
            return PreprocessingResult(preprocessed_text=text, passed=False)

        return PreprocessingResult(
            preprocessed_text=text,
            passed=True,
        )


# ── Step 5 — Language detection ───────────────────────────────────────────────

class LanguageDetectionStep(BasePreprocessor):
    """
    Detects document language using lingua-py.

    Builds the detector once at class level (expensive).
    Stores detected language and confidence in PreprocessingResult metadata.
    Always passes — an UNKNOWN language does not stop the pipeline.
    """

    # Built once for all instances — lingua is expensive to initialise
    _detector = LanguageDetectorBuilder.from_all_languages().build()

    MIN_TEXT_LENGTH = 20   # too short for reliable detection

    def process(self, text: str) -> PreprocessingResult:
        lang       = "UNKNOWN"
        confidence = 0.0

        if text and len(text.strip()) >= self.MIN_TEXT_LENGTH:
            try:
                results = self._detector.compute_language_confidence_values(text[:1000])
                if results:
                    top        = results[0]
                    lang       = top.language.name
                    confidence = round(top.value, 4)
            except Exception:
                logger.warning("LanguageDetectionStep: detection failed, defaulting to UNKNOWN")

        return PreprocessingResult(
            preprocessed_text=text,
            language=lang,
            lang_confidence=confidence,
            passed=True,
        )


# ── Step 6 — Deduplication ────────────────────────────────────────────────────

class DeduplicationStep(BasePreprocessor):
    """
    Two-layer deduplication:
        Layer 1 — exact match via SHA-256 hash (in-memory set).
        Layer 2 — near-duplicate via MinHash LSH (Jaccard threshold 0.85).

    Returns passed=False when a duplicate is detected.
    The pipeline stops and the record is marked SKIPPED_DUP.

    Note: for production persistence, swap _seen_hashes for a Redis-backed
    set and MinHashLSH for a Redis or Milvus 2.6 backend. The in-memory
    implementation is correct for single-process / test environments.
    """

    def __init__(self, threshold: float = 0.85, num_perm: int = 128) -> None:
        self._seen_hashes: set[str]  = set()
        self._lsh                    = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._num_perm               = num_perm
        self._doc_counter            = 0   # used as fallback doc_id

    def _hash(self, text: str) -> str:
        return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()

    def _minhash(self, text: str) -> MinHash:
        m        = MinHash(num_perm=self._num_perm)
        tokens   = text.lower().split()
        shingles = {
            " ".join(tokens[i : i + 3])
            for i in range(max(1, len(tokens) - 2))
        }
        for s in shingles:
            m.update(s.encode("utf-8"))
        return m

    def process(self, text: str) -> PreprocessingResult:
        # Layer 1 — exact
        h = self._hash(text)
        if h in self._seen_hashes:
            logger.debug("DeduplicationStep: exact duplicate detected")
            return PreprocessingResult(
                preprocessed_text=text,
                passed=False,
                is_duplicate=True,
            )
        self._seen_hashes.add(h)

        # Layer 2 — near-duplicate
        mh      = self._minhash(text)
        matches = self._lsh.query(mh)
        if matches:
            logger.debug("DeduplicationStep: near-duplicate detected, matched=%s", matches)
            return PreprocessingResult(
                preprocessed_text=text,
                passed=False,
                is_duplicate=True,
            )

        # Unique — register
        self._doc_counter += 1
        doc_id = f"doc_{self._doc_counter}"
        try:
            self._lsh.insert(doc_id, mh)
        except ValueError:
            pass  # already inserted — safe to ignore

        return PreprocessingResult(
            preprocessed_text=text,
            passed=True,
            is_duplicate=False,
        )
