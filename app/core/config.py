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
    SQL_ECHO: bool = False

    # ── App ───────────────────────────────────────────────────
    APP_NAME: str = "RAG Backend"
    VERSION: str = "1.0.0"

    # ── File Upload ───────────────────────────────────────────
    UPLOAD_DIR: Path = Path("uploads")
    MAX_FILE_SIZE_MB: int = 50

    def model_post_init(self, __context):
        self.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()