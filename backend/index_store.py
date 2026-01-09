"""Vector store indexing and retrieval."""

import json
from pathlib import Path
from typing import Optional, List, Dict
import numpy as np

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

from backend.entities import Company, DocumentChunk


class VectorIndex:
    """
    Manages vector embeddings and similarity search for document chunks.
    
    Uses FAISS for efficient similarity search and sentence-transformers
    for generating embeddings. Stores index and metadata separately.
    
    Representation Invariants:
    - index_path is an absolute Path
    - If index exists, metadata_path also exists
    - All chunks in index have corresponding metadata entries
    """
    
    def __init__(
        self,
        index_path: Path,
        embedding_model: str = "all-MiniLM-L6-v2"
    ) -> None:
        """
        Initialize vector index.
        
        Preconditions:
        - index_path is a valid Path (directory will be created if needed)
        - embedding_model is a valid sentence-transformers model name
        
        Postconditions:
        - Index directory exists
        - Embedding model is loaded (if available)
        
        Args:
            index_path: Directory where index files are stored
            embedding_model: Name of sentence-transformers model to use
        """
        if not FAISS_AVAILABLE:
            raise ImportError(
                "FAISS is not available. Install with: pip install faiss-cpu"
            )
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError(
                "sentence-transformers is not available. Install with: pip install sentence-transformers"
            )
        
        self._index_path = index_path.resolve()
        self._index_path.mkdir(parents=True, exist_ok=True)
        
        self._embedding_model_name = embedding_model
        self._embedding_model: Optional[SentenceTransformer] = None
        self._faiss_index: Optional[faiss.Index] = None
        self._chunk_metadata: List[Dict] = []
        self._dimension = 384  # Default for all-MiniLM-L6-v2
        
        # Load model (lazy loading in build_index)
    
    def _load_embedding_model(self) -> None:
        """Load the embedding model (lazy loading)."""
        if self._embedding_model is None:
            self._embedding_model = SentenceTransformer(self._embedding_model_name)
            # Get actual dimension from model
            test_embedding = self._embedding_model.encode(["test"])
            self._dimension = test_embedding.shape[1]
    
    def build_index(self, chunks: List[DocumentChunk]) -> None:
        """
        Build vector index from document chunks.
        
        Preconditions:
        - chunks is non-empty list
        - All chunks have valid text content
        
        Postconditions:
        - FAISS index is created and saved
        - Metadata is saved to JSON
        - Index is ready for search
        
        Args:
            chunks: List of DocumentChunk objects to index
        """
        if not chunks:
            raise ValueError("Cannot build index from empty chunk list")
        
        # Load embedding model
        self._load_embedding_model()
        
        # Extract texts and metadata
        texts = [chunk.text for chunk in chunks]
        self._chunk_metadata = [
            {
                "chunk_id": chunk.chunk_id,
                "ticker": chunk.source_filing.company.ticker,
                "accession": chunk.source_filing.accession,
                "period_end": chunk.source_filing.period_end,
                "chunk_index": chunk.chunk_index,
            }
            for chunk in chunks
        ]
        
        # Generate embeddings
        embeddings = self._embedding_model.encode(texts, show_progress_bar=False)
        embeddings = np.array(embeddings).astype('float32')
        
        # Create FAISS index (L2 distance)
        self._faiss_index = faiss.IndexFlatL2(self._dimension)
        self._faiss_index.add(embeddings)
        
        # Save index and metadata
        self._save_index()
    
    def _save_index(self) -> None:
        """Save FAISS index and metadata to disk."""
        if self._faiss_index is None:
            return
        
        # Save FAISS index
        index_file = self._index_path / "index.faiss"
        faiss.write_index(self._faiss_index, str(index_file))
        
        # Save metadata
        metadata_file = self._index_path / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(self._chunk_metadata, f, indent=2)
    
    def load_index(self) -> bool:
        """
        Load existing index from disk.
        
        Postconditions:
        - Returns True if index loaded successfully
        - Returns False if index doesn't exist
        - Raises ValueError if index is corrupted
        
        Returns:
            True if index loaded, False if doesn't exist
        """
        index_file = self._index_path / "index.faiss"
        metadata_file = self._index_path / "metadata.json"
        
        if not index_file.exists() or not metadata_file.exists():
            return False
        
        try:
            # Load FAISS index
            self._faiss_index = faiss.read_index(str(index_file))
            self._dimension = self._faiss_index.d
        
            # Load metadata
            with open(metadata_file, 'r') as f:
                self._chunk_metadata = json.load(f)
            
            # Load embedding model (needed for new queries)
            self._load_embedding_model()
            
            return True
        except Exception as e:
            raise ValueError(f"Failed to load index: {e}") from e
    
    def search(
        self,
        query: str,
        k: int = 5
    ) -> List[Dict]:
        """
        Search for similar chunks using semantic similarity.
        
        Preconditions:
        - Index is built or loaded
        - query is non-empty string
        - k > 0
        
        Postconditions:
        - Returns list of DocumentChunk objects (may be shorter than k if fewer available)
        - Results are sorted by similarity (most similar first)
        - Raises ValueError if index not loaded
        
        Args:
            query: Search query text
            k: Number of results to return
            
        Returns:
            List of DocumentChunk objects (reconstructed from metadata)
            
        Raises:
            ValueError: If index not loaded or query is empty
        """
        if self._faiss_index is None:
            raise ValueError("Index not loaded. Call build_index() or load_index() first.")
        
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if k <= 0:
            raise ValueError(f"k must be positive, got {k}")
        
        # Generate query embedding
        self._load_embedding_model()
        query_embedding = self._embedding_model.encode([query])
        query_embedding = np.array(query_embedding).astype('float32')
        
        # Search
        k = min(k, len(self._chunk_metadata))  # Don't ask for more than available
        distances, indices = self._faiss_index.search(query_embedding, k)
        
        # Reconstruct DocumentChunk objects from metadata
        # Note: We can't fully reconstruct without the original Filing objects,
        # so we return a simplified version with metadata
        results = []
        for idx, dist in zip(indices[0], distances[0]):
            if idx < len(self._chunk_metadata):
                meta = self._chunk_metadata[idx]
                # Create a minimal DocumentChunk (we'll need to store text separately)
                # For now, we'll need to modify this to store text in metadata
                # Or return metadata dicts instead
                results.append((meta, float(dist)))
        
        # Sort by distance (lower is better)
        results.sort(key=lambda x: x[1])
        
        # Return metadata dicts with scores
        return [{**meta, 'score': float(score)} for meta, score in results]
    
    def search_with_text(
        self,
        query: str,
        k: int = 5,
        chunk_texts: Optional[Dict[str, str]] = None
    ) -> List[Dict]:
        """
        Search and return results with text content.
        
        This is a convenience method that returns metadata dicts.
        The caller should maintain a mapping of chunk_id -> text if needed.
        
        Args:
            query: Search query
            k: Number of results
            chunk_texts: Optional dict mapping chunk_id -> text
            
        Returns:
            List of result dicts with metadata and optionally text
        """
        results = self.search(query, k)
        
        # Add text if provided
        if chunk_texts:
            for result in results:
                chunk_id = result["chunk_id"]
                if chunk_id in chunk_texts:
                    result["text"] = chunk_texts[chunk_id]
        
        return results


class IndexManager:
    """
    Manages multiple vector indexes (one per company/period).
    
    Provides a simple interface for storing and retrieving indexes
    for different companies and periods.
    """
    
    def __init__(self, base_path: Path) -> None:
        """
        Initialize index manager.
        
        Args:
            base_path: Base directory for all indexes
        """
        self._base_path = base_path.resolve()
        self._base_path.mkdir(parents=True, exist_ok=True)
    
    def get_index_path(self, company: Company, period_end: str) -> Path:
        """
        Get index path for a company and period.
        
        Args:
            company: Company entity
            period_end: Period end date (YYYY-MM-DD)
            
        Returns:
            Path to index directory
        """
        return self._base_path / company.ticker / period_end
    
    def get_or_create_index(
        self,
        company: Company,
        period_end: str,
        chunks: Optional[List[DocumentChunk]] = None
    ) -> VectorIndex:
        """
        Get existing index or create new one.
        
        FIXED: Automatically handles corrupted index files by deleting and recreating them.
        
        Args:
            company: Company entity
            period_end: Period end date
            chunks: Optional chunks to index (if creating new)
            
        Returns:
            VectorIndex instance
        """
        index_path = self.get_index_path(company, period_end)
        index = VectorIndex(index_path)
        
        # Try to load existing
        try:
            if index.load_index():
                return index
        except ValueError as e:
            # Index is corrupted - delete it and recreate
            import shutil
            print(f"     ⚠️  Corrupted index detected, recreating...")
            if index_path.exists():
                shutil.rmtree(index_path)
            index_path.mkdir(parents=True, exist_ok=True)
            # Create fresh index instance
            index = VectorIndex(index_path)
        
        # If chunks provided, build new index
        if chunks:
            index.build_index(chunks)
            return index
        
        # Return empty index (caller must build or load)
        return index
