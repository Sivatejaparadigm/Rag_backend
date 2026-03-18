from fastapi import APIRouter
# from app.services.parser import extract_text
# from app.services.preprocess import preprocess_text
# from app.services.chunker import chunk_text

router = APIRouter()

@router.post("/")
async def ingest_document():
    return {"message": "Hello World"}
    # # Step 1: Extract
    # raw_text = await extract_text(file)

    # # Step 2: Preprocess
    # cleaned_text = preprocess_text(raw_text)

    # # Step 3: Chunk
    # chunks = chunk_text(cleaned_text)

    # return {
    #     "filename": file.filename,
    #     "num_chunks": len(chunks),
    #     "chunks_preview": chunks[:3]  # preview only
    # }