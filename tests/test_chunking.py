"""Tests for document chunking."""

import pytest
from pathlib import Path
import tempfile

from backend.entities import Company, Filing
from backend.chunking import DocumentChunker
from backend.text_clean import TextExtractor


class TestDocumentChunker:
    """Test DocumentChunker class."""
    
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
        """Create a test filing with text file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            # Create a longer text with multiple paragraphs
            text = "\n\n".join([
                "This is paragraph one. It has some content about revenue.",
                "This is paragraph two. It discusses operating income.",
                "This is paragraph three. It mentions net income and earnings.",
                "This is paragraph four. It talks about guidance and outlook.",
                "This is paragraph five. It covers segment performance.",
            ])
            f.write(text)
            temp_path = Path(f.name)
        
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q",
            raw_text_path=temp_path
        )
        
        yield filing
        
        # Cleanup
        if temp_path.exists():
            temp_path.unlink()
    
    def test_init_valid_parameters(self):
        """Test initialization with valid parameters."""
        chunker = DocumentChunker(chunk_size=1000, chunk_overlap=200)
        assert chunker._chunk_size == 1000
        assert chunker._chunk_overlap == 200
    
    def test_init_invalid_chunk_size(self):
        """Test that invalid chunk_size raises error."""
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            DocumentChunker(chunk_size=0)
    
    def test_init_invalid_overlap(self):
        """Test that invalid overlap raises error."""
        with pytest.raises(ValueError, match="chunk_overlap must be non-negative"):
            DocumentChunker(chunk_size=1000, chunk_overlap=-1)
        
        with pytest.raises(ValueError, match="chunk_overlap.*must be < chunk_size"):
            DocumentChunker(chunk_size=1000, chunk_overlap=1000)
    
    def test_chunk_filing_creates_chunks(self, filing):
        """Test that chunking creates multiple chunks."""
        chunker = DocumentChunker(chunk_size=200, chunk_overlap=50)
        chunks = chunker.chunk_filing(filing)
        
        assert len(chunks) > 0
        # Chunks should be DocumentChunk objects
        from backend.entities import DocumentChunk
        assert all(isinstance(chunk, DocumentChunk) for chunk in chunks)
    
    def test_chunk_metadata_preserved(self, filing):
        """Test that chunk metadata is correctly set."""
        chunker = DocumentChunker(chunk_size=200, chunk_overlap=50)
        chunks = chunker.chunk_filing(filing)
        
        for i, chunk in enumerate(chunks):
            assert chunk.chunk_index == i
            assert chunk.source_filing == filing
            assert chunk.chunk_id.startswith("AAPL_")
            assert "chunk_" in chunk.chunk_id
            assert chunk.text  # Non-empty text
    
    def test_chunk_ids_are_unique(self, filing):
        """Test that chunk IDs are unique."""
        chunker = DocumentChunker(chunk_size=200, chunk_overlap=50)
        chunks = chunker.chunk_filing(filing)
        
        chunk_ids = [chunk.chunk_id for chunk in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))  # All unique
    
    def test_chunk_size_respected(self, filing):
        """Test that chunks respect maximum size (approximately)."""
        # Use smaller min_chunk_size so test paragraphs can form chunks
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=20, min_chunk_size=50)
        chunks = chunker.chunk_filing(filing)
        
        # Chunks should be roughly around chunk_size (allow some flexibility)
        for chunk in chunks:
            # Chunk size can be slightly larger due to paragraph boundaries
            assert len(chunk.text) <= chunker._chunk_size * 1.5  # Allow 50% overflow
    
    def test_chunks_meet_minimum_size(self, filing):
        """Test that all chunks meet minimum size requirement."""
        chunker = DocumentChunker(chunk_size=200, chunk_overlap=50, min_chunk_size=50)
        chunks = chunker.chunk_filing(filing)
        
        for chunk in chunks:
            assert len(chunk.text) >= chunker._min_chunk_size
    
    def test_chunk_filing_with_none_path_raises_error(self, company):
        """Test that filing without raw_text_path raises error."""
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q",
            raw_text_path=None
        )
        
        chunker = DocumentChunker()
        with pytest.raises(ValueError, match="has no raw_text_path"):
            chunker.chunk_filing(filing)
    
    def test_chunk_ordering(self, filing):
        """Test that chunks are in document order."""
        chunker = DocumentChunker(chunk_size=200, chunk_overlap=50)
        chunks = chunker.chunk_filing(filing)
        
        # Check that chunk indices are sequential
        indices = [chunk.chunk_index for chunk in chunks]
        assert indices == list(range(len(chunks)))
        
        # Check that first chunk contains early content
        assert "paragraph one" in chunks[0].text.lower() or "revenue" in chunks[0].text.lower()
    
    def test_overlap_between_chunks(self, filing):
        """Test that chunks have overlap when configured."""
        chunker = DocumentChunker(chunk_size=150, chunk_overlap=50)
        chunks = chunker.chunk_filing(filing)
        
        if len(chunks) > 1:
            # Check that consecutive chunks share some content
            # (This is approximate - overlap might be at boundaries)
            chunk1_text = chunks[0].text.lower()
            chunk2_text = chunks[1].text.lower()
            
            # They should share some words (simple check)
            words1 = set(chunk1_text.split())
            words2 = set(chunk2_text.split())
            # At least some overlap in words
            assert len(words1 & words2) > 0 or len(chunks) == 1
    
    def test_empty_text_raises_error(self, company):
        """Test that empty text raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("   \n\n\n   ")  # Only whitespace
            temp_path = Path(f.name)
        
        try:
            filing = Filing(
                company=company,
                accession="0000320193-23-000077",
                filing_date="2023-11-03",
                period_end="2023-09-30",
                filing_type="10-Q",
                raw_text_path=temp_path
            )
            
            chunker = DocumentChunker()
            # Should raise error during text extraction or chunking
            with pytest.raises((ValueError,)):
                chunker.chunk_filing(filing)
        finally:
            if temp_path.exists():
                temp_path.unlink()
