import os
import re
import hashlib
import unicodedata
import warnings
import logging

from ftfy import fix_text
from lingua import LanguageDetectorBuilder
from datasketch import MinHash, MinHashLSH
from langchain_community.document_loaders import UnstructuredFileLoader, PyPDFLoader
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.chunk_repository import ExtractedContentRepository, PreprocessedDocumentRepository
from app.schemas.chunk_schemas import PreprocessedDocumentCreate, PreprocessedDocumentUpdate

# ── Path — use os.path.join to avoid Windows backslash issues ──────────────────
import os
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
FOLDER_PATH = os.path.join(BASE_DIR, "parser_test")    # ✅ no backslash errors

SUPPORTED_EXT = (".pdf", ".docx", ".txt", ".html", ".eml", ".csv")

# ==============================================================================
# STEP 1 — Load documents
# ==============================================================================
def load_documents(folder_path: str) -> list:
    files = [
        f for f in os.listdir(folder_path)
        if f.lower().endswith(SUPPORTED_EXT)
    ]

    all_docs = []

    print("=" * 60)
    print("📂 LOADING DOCUMENTS")
    print("=" * 60)

    for file in files:
        path = os.path.join(folder_path, file)
        try:
            if file.lower().endswith(".pdf"):
                loader = PyPDFLoader(path)               # no Tesseract needed
            else:
                loader = UnstructuredFileLoader(path, mode="single")

            docs = loader.load()
            all_docs.extend(docs)
            print(f"✅ Loaded : {file:40s} → {len(docs)} chunk(s)")

        except Exception as e:
            print(f"❌ Failed : {file} → {e}")

    print("=" * 60)
    print(f"✅ Total documents loaded: {len(all_docs)}")
    print("=" * 60)
    return all_docs


# ==============================================================================
# STEP 2 — Text cleaning helpers
# ==============================================================================

# Compiled regex patterns (compile once, reuse)
_space_pattern   = re.compile(r"[ \t]+")    # collapse spaces/tabs → single space
_newline_pattern = re.compile(r"\n+")       # collapse 2+ newlines → single newline


def clean_whitespace(text: str) -> str:
    """Collapse extra spaces, tabs and newlines."""
    text = _space_pattern.sub(" ", text)
    text = _newline_pattern.sub("\n", text)
    return text.strip()


# ==============================================================================
# STEP 3 — Junk / noise filter
# ==============================================================================

MIN_WORDS       = 5
MAX_SYMBOL_RATIO = 0.4
MAX_DIGIT_RATIO  = 0.6


def is_junk(text: str) -> bool:
    """Return True if the text is noise that should be dropped."""
    if not text or not text.strip():
        return True

    words  = text.split()
    total  = len(text)

    if len(words) < MIN_WORDS:
        return True

    # Too many non-alphanumeric symbols
    symbol = sum(1 for c in text if not c.isalnum() and not c.isspace())
    if total > 0 and symbol / total > MAX_SYMBOL_RATIO:
        return True

    # Too many digits — likely a table row or page number
    digit  = sum(1 for c in text if c.isdigit())
    if total > 0 and digit / total > MAX_DIGIT_RATIO:
        return True

    return False


def filter_doc(text: str) -> tuple[str, bool]:
    """Returns (text, kept). Drops junk lines from multi-line text."""
    lines   = text.splitlines()
    kept    = [ln for ln in lines if not is_junk(ln)]
    cleaned = "\n".join(kept).strip()
    return cleaned, bool(cleaned)


# ==============================================================================
# STEP 4 — Language detection
# ==============================================================================

# Build detector once at module level (expensive to build)
print("🔤 Building language detector…")
_detector = LanguageDetectorBuilder.from_all_languages().build()
print("✅ Language detector ready")


def detect_language(text: str) -> dict:
    """
    Returns dict: {"lang": "ENGLISH", "confidence": 0.99}
    Always returns a dict — pipeline can safely use ["lang"] and ["confidence"].
    """
    if not text or len(text.strip()) < 20:
        return {"lang": "UNKNOWN", "confidence": 0.0}

    try:
        results = _detector.compute_language_confidence_values(text[:1000])
        if results:
            top    = results[0]          # highest confidence result
            return {
                "lang":       top.language.name,
                "confidence": round(top.value, 4),
            }
    except Exception:
        pass

    return {"lang": "UNKNOWN", "confidence": 0.0}


# ==============================================================================
# STEP 5 — Deduplication (exact + near-duplicate via MinHash LSH)
# ==============================================================================

class Deduplicator:
    def __init__(self, threshold: float = 0.85, num_perm: int = 128):
        self.seen_hashes = set()
        self.lsh         = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self.num_perm    = num_perm

    def _get_hash(self, text: str) -> str:
        return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()

    def _make_minhash(self, text: str) -> MinHash:
        m        = MinHash(num_perm=self.num_perm)
        tokens   = text.lower().split()
        shingles = {
            " ".join(tokens[i : i + 3])
            for i in range(max(1, len(tokens) - 2))
        }
        for s in shingles:
            m.update(s.encode("utf-8"))
        return m

    def is_duplicate(self, doc_id: str, text: str) -> dict:
        # Step 1 — exact match via SHA-256
        h = self._get_hash(text)
        if h in self.seen_hashes:
            return {"is_dup": True, "type": "exact", "matched": "seen_hash"}
        self.seen_hashes.add(h)

        # Step 2 — near-duplicate via MinHash LSH
        mh      = self._make_minhash(text)
        matches = self.lsh.query(mh)
        if matches:
            return {"is_dup": True, "type": "near", "matched": matches}

        # Unique — register in LSH index
        try:
            self.lsh.insert(doc_id, mh)
        except ValueError:
            pass  # already inserted (safe to ignore)

        return {"is_dup": False, "type": "none", "matched": []}


# ==============================================================================
# STEP 6 — Full pipeline
# ==============================================================================

def run_pipeline(docs: list) -> list:
    """
    Runs all cleaning steps on a list of LangChain Document objects.
    Returns the cleaned, deduplicated list.
    """
    dedup = Deduplicator(threshold=0.85)
    clean = []

    for doc in docs:
        # 1. Fix broken encoding (ftfy)
        doc.page_content = fix_text(doc.page_content)

        # 2. Unicode normalisation — ligatures, subscripts, full-width chars
        doc.page_content = unicodedata.normalize("NFKC", doc.page_content)

        # 3. Collapse extra whitespace
        doc.page_content = clean_whitespace(doc.page_content)

        # 4. Drop junk lines
        doc.page_content, kept = filter_doc(doc.page_content)
        if not kept:
            continue   # entire doc was junk — skip

        # 5. Language detection — returns dict {"lang": ..., "confidence": ...}
        lang_result = detect_language(doc.page_content)
        doc.metadata["language"]        = lang_result["lang"]
        doc.metadata["lang_confidence"] = lang_result["confidence"]

        # 6. Deduplication
        doc_id = doc.metadata.get("source", str(id(doc)))
        result = dedup.is_duplicate(doc_id, doc.page_content)
        doc.metadata["is_duplicate"] = result["is_dup"]
        doc.metadata["dup_type"]     = result["type"]

        if not result["is_dup"]:
            clean.append(doc)

    print(f"\n✅ Pipeline done — {len(clean)}/{len(docs)} docs passed")
    return clean


# ==============================================================================
# MAIN — run everything
# ==============================================================================

if __name__ == "__main__":
    # Load
    docs = load_documents(FOLDER_PATH)

    # Run pipeline
    docs = run_pipeline(docs)

    # Print results
    print("\n📄 CLEANED DOCUMENT CONTENTS\n")
    for i, doc in enumerate(docs):
        src  = doc.metadata.get("source", "N/A")
        lang = doc.metadata.get("language", "N/A")
        conf = doc.metadata.get("lang_confidence", 0)
        dup  = doc.metadata.get("is_duplicate", False)

        print(f"{'─' * 60}")
        print(f"📌 Doc #{i + 1}")
        print(f"   Source      : {src}")
        print(f"   Language    : {lang} (confidence: {conf})")
        print(f"   Duplicate   : {dup}")
        print(f"   Length      : {len(doc.page_content)} chars")
        print(f"\n   Preview (500 chars):")
        print(f"   {doc.page_content[:500].strip()}")
        print()


def apply_preprocessing(raw_text: str) -> dict:
    """Apply your 6-step preprocessing to raw text and return result info."""
    normalized = fix_text(raw_text or "")
    normalized = unicodedata.normalize("NFKC", normalized)
    normalized = clean_whitespace(normalized)
    normalized, kept = filter_doc(normalized)

    lang = detect_language(normalized)
    word_count = len(normalized.split()) if normalized else 0

    return {
        "preprocessed_text": normalized,
        "language": lang.get("lang", "UNKNOWN"),
        "lang_confidence": lang.get("confidence", 0.0),
        "word_count": word_count,
        "passed": kept
    }


async def preprocess_extracted_content(db: AsyncSession, job_id):
    """Load ExtractedContent by job_id, run preprocess, save into PreprocessedDocument."""
    extracted_repo = ExtractedContentRepository(db)
    preprocessed_repo = PreprocessedDocumentRepository(db)

    extracted = await extracted_repo.get_content_by_job_id(job_id)
    if not extracted:
        raise ValueError(f"ExtractedContent not found for job_id={job_id}")

    result = apply_preprocessing(extracted.raw_text or "")
    if not result["passed"]:
        raise ValueError(f"Preprocessing dropped content for job_id={job_id}")

    preprocessed_doc_data = {
        "source_id": extracted.id,
        "full_text": extracted.raw_text or "",
        "preprocessed_text": result["preprocessed_text"],
        "page_count": len(extracted.pages) if extracted.pages else None,
        "sections": None,
        "tables": extracted.tables,
        "language": result["language"],
        "has_ocr": False,
        "parse_method": "extracted_content_pipeline",
        "word_count": result["word_count"],
        "preprocessed_at": datetime.utcnow()
    }

    existing_doc = await preprocessed_repo.get_document_by_source(extracted.id)
    if existing_doc:
        # Update existing entry (keep parse_id, parsed_at)
        await preprocessed_repo.update_document(
            existing_doc.parse_id,
            PreprocessedDocumentUpdate(
                preprocessed_text=preprocessed_doc_data["preprocessed_text"],
                page_count=preprocessed_doc_data["page_count"],
                sections=preprocessed_doc_data["sections"],
                tables=preprocessed_doc_data["tables"],
                language=preprocessed_doc_data["language"],
                has_ocr=preprocessed_doc_data["has_ocr"],
                parse_method=preprocessed_doc_data["parse_method"],
                word_count=preprocessed_doc_data["word_count"],
                preprocessed_at=preprocessed_doc_data["preprocessed_at"]
            )
        )
        preprocessed_doc = await preprocessed_repo.get_document_by_id(existing_doc.parse_id)
    else:
        preprocessed_doc = await preprocessed_repo.create_document(PreprocessedDocumentCreate(**preprocessed_doc_data))

    return {
        "preprocessed_document": preprocessed_doc,
        "chunks_created": 0
    }