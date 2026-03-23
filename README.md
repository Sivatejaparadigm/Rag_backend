## RAG Backend (Ingestion + Postgres)

### What’s included
- **PostgreSQL** via `docker-compose.yml`
- **SQLAlchemy (async)** DB layer in `app/core/database.py`
- **Alembic** migrations in `alembic/`
- **First ingestion endpoint**: `POST /ingest/upload` (PDF/TXT) stores the uploaded file + a DB row

### Quickstart
Create your env file:

- Copy `.env.example` to `.env` and edit as needed.

Start Postgres:

```bash
docker compose up -d
```

Install deps:

```bash
python -m pip install -r requirements.txt
```

Run migrations (create tables):

```bash
alembic upgrade head
```

Run the API:

```bash
uvicorn app.main:app --reload
```

### Test upload

```bash
curl -X POST "http://127.0.0.1:8000/ingest/upload" -F "file=@./some.pdf"
```

