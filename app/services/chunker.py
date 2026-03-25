from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma

# ── Correct imports for your version (langchain-classic 1.0.2) ────
from langchain_classic.retrievers import ParentDocumentRetriever   # ← langchain_classic
from langchain_classic.storage import InMemoryStore                # ← langchain_classic

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate

import uuid
import os
from dotenv import load_dotenv

load_dotenv();

class Chunker:
    def __init__(self, text):
        self.text = text.strip()

    def fixed_size_chunking(self, chunk_size=500, chunk_overlap=100):
        """
        Fixed-size chunking with overlap.
        """
        fixed_splitter = CharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separator="\n"
        )
        return fixed_splitter.create_documents([self.text])

    def semantic_chunking(self, model_name="sentence-transformers/all-MiniLM-L6-v2", breakpoint_threshold_type="percentile", breakpoint_threshold_amount=90):
        """
        Semantic chunking using embeddings.
        """
        embeddings = HuggingFaceEmbeddings(model_name=model_name)
        semantic_splitter = SemanticChunker(
            embeddings,
            breakpoint_threshold_type=breakpoint_threshold_type,
            breakpoint_threshold_amount=breakpoint_threshold_amount
        )
        return semantic_splitter.create_documents([self.text])

    def recursive_chunking(self, chunk_size=500, chunk_overlap=100, separators=["\n\n", "\n", ".", " ", ""]):
        """
        Recursive / Hierarchical chunking.
        """
        recursive_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=separators
        )
        return recursive_splitter.create_documents([self.text])

    def agentic_chunking(self, model="gemini-2.5-flash", temperature=0, max_length=2000):
        """
        Agentic (LLM-driven) chunking using Gemini.
        """
        google_api_key = os.environ.get('GOOGLE_API_KEY')
        if not google_api_key:
            raise ValueError("Google API key is required for agentic chunking. Please set GOOGLE_API_KEY in your environment.")

        llm = ChatGoogleGenerativeAI(
            model=model,
            google_api_key=google_api_key,
            temperature=temperature
        )

        agentic_prompt = PromptTemplate.from_template("""
        You are a document chunking expert.
        Split the following text into meaningful chunks based on topic and context.
        Rules:
        - Each chunk must be self-contained and focused on ONE topic
        - Keep related sentences together
        - Do NOT summarize or modify the text
        - Separate each chunk with exactly: ---CHUNK---

        TEXT:
        {text}

        Return ONLY the chunks separated by ---CHUNK--- and nothing else.
        """)

        input_text = self.text[:max_length]  # limit for demo

        response = llm.invoke(agentic_prompt.format(text=input_text))

        agentic_chunks = [
            c.strip() for c in response.content.split("---CHUNK---")
            if c.strip()
        ]

        return agentic_chunks

    def parent_child_chunking(self, parent_chunk_size=1000, parent_overlap=100, child_chunk_size=200, child_overlap=20):
        """
        Parent-child chunking.
        Returns a list of child documents with parent metadata.
        """
        parent_splitter = RecursiveCharacterTextSplitter(chunk_size=parent_chunk_size, chunk_overlap=parent_overlap)
        child_splitter = RecursiveCharacterTextSplitter(chunk_size=child_chunk_size, chunk_overlap=child_overlap)

        parent_docs = parent_splitter.create_documents([self.text])

        child_docs_with_parent = []

        for i, parent in enumerate(parent_docs):
            parent_id = str(uuid.uuid4())
            parent.metadata["doc_id"] = parent_id

            children = child_splitter.create_documents(
                [parent.page_content],
                metadatas=[{"parent_id": parent_id, "parent_index": i}]
            )
            child_docs_with_parent.extend(children)

        return parent_docs, child_docs_with_parent

# Sample usage
if __name__ == "__main__":
    sample_text = """
    Retrieval-Augmented Generation (RAG) is a powerful AI framework that combines
    the strengths of large language models (LLMs) with external knowledge retrieval.
    Instead of relying solely on the model's training data, RAG systems fetch relevant
    documents from a knowledge base and use them to generate more accurate, up-to-date responses.

    The core RAG pipeline consists of several key stages. First, documents are ingested
    and preprocessed — this includes loading files of various formats such as PDFs, Word
    documents, HTML pages, and plain text files. Each document is cleaned, normalized,
    and prepared for further processing.

    The second stage is chunking, where large documents are split into smaller, manageable
    pieces. This is critical because embedding models and LLMs have token limits. Poor
    chunking leads to lost context or irrelevant retrievals. Common strategies include
    fixed-size chunking, recursive chunking, semantic chunking, and parent-child chunking.

    Fixed-size chunking splits text into equal-sized pieces with optional overlap. It is
    simple and fast but may cut sentences mid-way. Recursive chunking is smarter — it
    tries to split on natural boundaries like paragraphs, then sentences, then words.
    This preserves context better than fixed-size splitting.

    Semantic chunking uses embedding models to detect topic shifts in the text. When the
    semantic similarity between consecutive sentences drops below a threshold, a new chunk
    begins. This produces chunks that are topically coherent and work well for dense
    technical documents with multiple distinct topics.

    Parent-child chunking is specifically designed for RAG pipelines. Small child chunks
    are indexed in the vector store for precise retrieval, but when a match is found,
    the larger parent chunk is returned to the LLM. This gives the model more context
    while keeping retrieval accurate.

    Agentic chunking uses an LLM itself to decide where to split the document. The model
    reads the text and identifies natural topic boundaries, producing highly intelligent
    chunks. However, this approach is slower and more expensive than rule-based methods.

    After chunking, each chunk is converted into a vector embedding using a model like
    sentence-transformers or OpenAI embeddings. These embeddings capture the semantic
    meaning of each chunk as a dense numerical vector.

    The embeddings are stored in a vector database such as Chroma, FAISS, Pinecone, or
    Weaviate. When a user query arrives, it is also embedded and compared against stored
    vectors using cosine similarity or dot product to find the most relevant chunks.

    Retrieved chunks are then passed to the LLM as context along with the original query.
    The LLM uses this context to generate a grounded, accurate answer. This approach
    significantly reduces hallucinations compared to using the LLM alone.

    Advanced RAG systems include re-ranking, query expansion, HyDE (Hypothetical Document
    Embeddings), and multi-hop retrieval. These techniques further improve retrieval quality
    and answer accuracy for complex, multi-part questions.
    """

    chunker = Chunker(sample_text)

    # Example: Fixed-size chunking
    fixed_chunks = chunker.fixed_size_chunking()
    print(f"Fixed-size chunks: {len(fixed_chunks)}")

    # Example: Semantic chunking
    semantic_chunks = chunker.semantic_chunking()
    print(f"Semantic chunks: {len(semantic_chunks)}")

    # Example: Recursive chunking
    recursive_chunks = chunker.recursive_chunking()
    print(f"Recursive chunks: {len(recursive_chunks)}")

    # Example: Agentic chunking
    agentic_chunks = chunker.agentic_chunking()
    print(f"Agentic chunks: {len(agentic_chunks)}")

    # Example: Parent-child chunking
    parents, children = chunker.parent_child_chunking()
    print(f"Parent chunks: {len(parents)}, Child chunks: {len(children)}")