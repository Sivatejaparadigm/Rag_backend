from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )

    # ── Database ──────────────────────────────────────────────
    DATABASE_URL: str
<<<<<<< HEAD
    GOOGLE_API_KEY: str
=======
>>>>>>> f425f686a1d9fa7ceb4ac42affb0b118e08c77a3
    SQL_ECHO: bool = False

    # ── App ───────────────────────────────────────────────────
    APP_NAME: str = "RAG Backend"
    VERSION: str = "1.0.0"

    # ── File Upload ───────────────────────────────────────────
    UPLOAD_DIR: Path = Path("uploads")
    MAX_FILE_SIZE_MB: int = 50
<<<<<<< HEAD
=======
    MAX_BATCH_FILES: int = 20

    # ── Allowed File Types ────────────────────────────────────
    ALLOWED_EXTENSIONS: set = {
        ".pdf", ".docx", ".doc", ".pptx",
        ".xlsx", ".xls", ".txt", ".html",
        ".htm", ".md", ".rtf", ".csv",
    }
>>>>>>> f425f686a1d9fa7ceb4ac42affb0b118e08c77a3

    def model_post_init(self, __context):
        self.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()