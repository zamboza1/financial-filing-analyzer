"""Tests for vector store indexing and retrieval."""

import pytest
from pathlib import Path
import tempfile

from backend.entities import Company, Filing, DocumentChunk
from backend.index_store import VectorIndex, IndexManager


class TestVectorIndex:
    """Test VectorIndex class."""
    
    @pytest.fixture
    def company(self):
        """Create a test company."""
        return Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="320193",
            peers=[]
        )
    
    @pytest.fixture
    def filing(self, company):
        """Create a test filing."""
        return Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q"
        )
    
    @pytest.fixture
    def chunks(self, filing):
        """Create test document chunks."""
        return [
            DocumentChunk(
                chunk_id="AAPL_0000320193_23_000077_chunk_0",
                text="Revenue for the quarter was $89.5 billion, up 1% year-over-year.",
                source_filing=filing,
                chunk_index=0
            ),
            DocumentChunk(
                chunk_id="AAPL_0000320193_23_000077_chunk_1",
                text="Operating income reached $26.9 billion with operating margin of 30%.",
                source_filing=filing,
                chunk_index=1
            ),
            DocumentChunk(
                chunk_id="AAPL_0000320193_23_000077_chunk_2",
                text="Net income was $22.96 billion, resulting in earnings per share of $1.46.",
                source_filing=filing,
                chunk_index=2
            ),
        ]
    
    @pytest.fixture
    def index_path(self):
        """Create temporary directory for index."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_init_creates_directory(self, index_path):
        """Test that initialization creates index directory."""
        index = VectorIndex(index_path)
        assert index_path.exists()
        assert index_path.is_dir()
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_build_index_creates_files(self, index_path, chunks):
        """Test that building index creates files."""
        try:
            index = VectorIndex(index_path)
            index.build_index(chunks)
            
            assert (index_path / "index.faiss").exists()
            assert (index_path / "metadata.json").exists()
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_build_index_stores_metadata(self, index_path, chunks):
        """Test that metadata is stored correctly."""
        try:
            index = VectorIndex(index_path)
            index.build_index(chunks)
            
            import json
            with open(index_path / "metadata.json") as f:
                metadata = json.load(f)
            
            assert len(metadata) == len(chunks)
            assert metadata[0]["chunk_id"] == chunks[0].chunk_id
            assert metadata[0]["ticker"] == "AAPL"
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_load_index(self, index_path, chunks):
        """Test loading an existing index."""
        try:
            # Build index
            index1 = VectorIndex(index_path)
            index1.build_index(chunks)
            
            # Load in new instance
            index2 = VectorIndex(index_path)
            loaded = index2.load_index()
            
            assert loaded is True
            assert len(index2._chunk_metadata) == len(chunks)
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_load_index_nonexistent_returns_false(self, index_path):
        """Test that loading non-existent index returns False."""
        try:
            index = VectorIndex(index_path)
            loaded = index.load_index()
            assert loaded is False
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_search_returns_results(self, index_path, chunks):
        """Test that search returns results."""
        try:
            index = VectorIndex(index_path)
            index.build_index(chunks)
            
            results = index.search("revenue", k=2)
            
            assert len(results) > 0
            assert len(results) <= 2
            assert all("chunk_id" in result for result in results)
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_search_before_build_raises_error(self, index_path):
        """Test that searching before building raises error."""
        try:
            index = VectorIndex(index_path)
            with pytest.raises(ValueError, match="Index not loaded"):
                index.search("test query")
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_search_empty_query_raises_error(self, index_path, chunks):
        """Test that empty query raises error."""
        try:
            index = VectorIndex(index_path)
            index.build_index(chunks)
            
            with pytest.raises(ValueError, match="Query cannot be empty"):
                index.search("")
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_search_semantic_similarity(self, index_path, chunks):
        """Test that search returns semantically similar results."""
        try:
            index = VectorIndex(index_path)
            index.build_index(chunks)
            
            # Search for "earnings" - should find the chunk about net income/EPS
            results = index.search("earnings per share", k=1)
            
            assert len(results) > 0
            # The result should be related to earnings
            result_chunk_id = results[0]["chunk_id"]
            # Find the corresponding chunk
            matching_chunk = next(c for c in chunks if c.chunk_id == result_chunk_id)
            assert "earnings" in matching_chunk.text.lower() or "eps" in matching_chunk.text.lower()
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")
    
    def test_build_index_empty_chunks_raises_error(self, index_path):
        """Test that building index with empty chunks raises error."""
        try:
            index = VectorIndex(index_path)
            with pytest.raises(ValueError, match="Cannot build index from empty"):
                index.build_index([])
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")


class TestIndexManager:
    """Test IndexManager class."""
    
    @pytest.fixture
    def company(self):
        """Create a test company."""
        return Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="320193",
            peers=[]
        )
    
    @pytest.fixture
    def base_path(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_get_index_path(self, company, base_path):
        """Test index path generation."""
        manager = IndexManager(base_path)
        path = manager.get_index_path(company, "2023-09-30")
        
        assert "AAPL" in str(path)
        assert "2023-09-30" in str(path)
    
    @pytest.mark.skipif(
        not VectorIndex.__module__ or 'faiss' not in str(VectorIndex.__module__),
        reason="FAISS not available"
    )
    def test_get_or_create_index_creates_new(self, company, base_path):
        """Test creating a new index."""
        try:
            manager = IndexManager(base_path)
            filing = Filing(
                company=company,
                accession="0000320193-23-000077",
                filing_date="2023-11-03",
                period_end="2023-09-30",
                filing_type="10-Q"
            )
            chunks = [
                DocumentChunk(
                    chunk_id="test_chunk_0",
                    text="Test content",
                    source_filing=filing,
                    chunk_index=0
                )
            ]
            
            index = manager.get_or_create_index(company, "2023-09-30", chunks)
            
            assert index is not None
            index_path = manager.get_index_path(company, "2023-09-30")
            assert (index_path / "index.faiss").exists()
        except ImportError:
            pytest.skip("FAISS or sentence-transformers not available")



