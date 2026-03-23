from __future__ import annotations

import re
from pathlib import Path


UPLOADS_DIR = Path("uploads")


_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def ensure_uploads_dir() -> Path:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    return UPLOADS_DIR


def sanitize_filename(filename: str) -> str:
    name = Path(filename).name  # drop any path components
    name = _FILENAME_SAFE_RE.sub("_", name).strip("._")
    return name or "file"

