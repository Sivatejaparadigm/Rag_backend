"""
Test script for all chunking strategies.
Tests: recursive, fixed, semantic, agentic, parent_child
"""
import asyncio
import uuid
from app.services.chunking_service import ChunkingService
from app.schemas.chunking_schemas import ChunkStrategy
from app.models.preprocessor import PreprocessedData

# Mock repository for testing
class MockChunkRepository:
    async def create_many(self, items):
        print(f"  ✅ Would save {len(items)} chunks to database")
        return items

async def test_all_strategies():
    """Test all chunking strategies."""
    
    # Create mock preprocessed data
    sample_text = """
    Natural Language Processing is a subfield of linguistics, computer science, and artificial intelligence 
    concerned with the interactions between computers and human language. NLP is used to apply machine learning 
    algorithms to text and speech.
    
    Some popular NLP tasks include sentiment analysis, machine translation, question answering, and named entity 
    recognition. These tasks have numerous real-world applications in chatbots, virtual assistants, and search engines.
    
    Deep learning models like BERT and GPT have revolutionized NLP. These transformer-based models can understand 
    context and nuance much better than previous statistical approaches. They form the foundation of modern language AI.
    
    Text preprocessing is crucial for NLP tasks. This includes tokenization, lemmatization, and removing stopwords. 
    Proper preprocessing can significantly improve model performance. Different tasks may require different preprocessing steps.
    """
    
    # Create a mock preprocessed record
    mock_record = PreprocessedData(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        content_id=uuid.uuid4(),
        preprocessed_text=sample_text,
    )
    
    # Initialize service
    repo = MockChunkRepository()
    service = ChunkingService(repo)
    
    strategies = [
        ChunkStrategy.RECURSIVE,
        ChunkStrategy.FIXED,
        ChunkStrategy.SEMANTIC,
        ChunkStrategy.AGENTIC,
        ChunkStrategy.PARENT_CHILD,
    ]
    
    print("\n" + "="*70)
    print("CHUNKING STRATEGY TESTS")
    print("="*70)
    
    for strategy in strategies:
        print(f"\n📊 Testing {strategy.value.upper()} CHUNKING")
        print("-" * 70)
        
        try:
            chunks = service._apply_strategy(
                text=sample_text,
                strategy=strategy,
                chunk_size=300,
                chunk_overlap=50,
            )
            
            print(f"  Success! Created {len(chunks)} chunks")
            
            # Show sample chunks
            for i, chunk in enumerate(chunks[:2], 1):
                preview = chunk[:100] + "..." if len(chunk) > 100 else chunk
                print(f"  Chunk {i}: {repr(preview)}")
            
            if len(chunks) > 2:
                print(f"  ... and {len(chunks) - 2} more chunks")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
    
    print("\n" + "="*70)
    print("FULL JOB CHUNKING TEST")
    print("="*70)
    
    # Test chunk_job with all strategies
    for strategy in strategies:
        print(f"\n📥 Chunking job with {strategy.value.upper()}")
        try:
            chunks_created = await service.chunk_job(
                job_id=uuid.uuid4(),
                session_id=uuid.uuid4(),
                preprocessed_records=[mock_record],
                strategy=strategy,
                chunk_size=300,
                chunk_overlap=50,
            )
            print(f"  ✅ Created {chunks_created} chunks for job")
        except Exception as e:
            print(f"  ❌ Error during chunking: {str(e)}")
    
    print("\n" + "="*70)
    print("ALL TESTS COMPLETED")
    print("="*70 + "\n")

if __name__ == "__main__":
    asyncio.run(test_all_strategies())
