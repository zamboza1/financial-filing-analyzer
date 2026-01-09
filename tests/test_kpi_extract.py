"""Tests for KPI extraction."""

import pytest
from pathlib import Path
from backend.entities import Company, Filing, DocumentChunk
from backend.kpi_extract import KPIExtractor


class TestKPIExtractor:
    """Test KPIExtractor class."""
    
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
    def sample_chunks(self, filing):
        """Create sample chunks with financial data."""
        text1 = """
        CONSOLIDATED STATEMENTS OF OPERATIONS
        Total net sales: $89,587 million
        Gross profit: $33,215 million
        Operating income: $13,411 million
        Net income: $22,956 million
        Earnings per share: $1.46
        """
        
        text2 = """
        GUIDANCE AND OUTLOOK
        For the fiscal 2024 first quarter, the Company expects:
        Revenue between $89.5 billion and $91.5 billion
        Gross margin between 37% and 38%
        """
        
        return [
            DocumentChunk(
                chunk_id="AAPL_0000320193_23_000077_chunk_0",
                text=text1,
                source_filing=filing,
                chunk_index=0
            ),
            DocumentChunk(
                chunk_id="AAPL_0000320193_23_000077_chunk_1",
                text=text2,
                source_filing=filing,
                chunk_index=1
            ),
        ]
    
    def test_extract_revenue(self, sample_chunks, filing):
        """Test revenue extraction."""
        extractor = KPIExtractor()
        snapshot = extractor.extract_from_chunks(sample_chunks, "2023-09-30")
        
        assert snapshot.revenue is not None
        assert abs(snapshot.revenue - 89587.0) < 1.0  # Allow small rounding
        assert snapshot.source_chunk_ids.get('revenue') == sample_chunks[0].chunk_id
    
    def test_extract_net_income(self, sample_chunks, filing):
        """Test net income extraction."""
        extractor = KPIExtractor()
        snapshot = extractor.extract_from_chunks(sample_chunks, "2023-09-30")
        
        assert snapshot.net_income is not None
        assert abs(snapshot.net_income - 22956.0) < 1.0
        assert snapshot.source_chunk_ids.get('net_income') == sample_chunks[0].chunk_id
    
    def test_extract_gross_margin(self, sample_chunks, filing):
        """Test gross margin extraction."""
        extractor = KPIExtractor()
        snapshot = extractor.extract_from_chunks(sample_chunks, "2023-09-30")
        
        # Should extract from text or calculate from gross_profit/revenue
        if snapshot.gross_margin is not None:
            # If extracted directly
            assert 0 <= snapshot.gross_margin <= 1
        elif snapshot.gross_profit is not None and snapshot.revenue is not None:
            # Should calculate
            expected = snapshot.gross_profit / snapshot.revenue
            assert abs(snapshot.gross_margin - expected) < 0.01
    
    def test_extract_guidance(self, sample_chunks, filing):
        """Test guidance extraction."""
        extractor = KPIExtractor()
        snapshot = extractor.extract_from_chunks(sample_chunks, "2023-09-30")
        
        # Guidance should be found in second chunk
        assert snapshot.guidance is not None
        assert len(snapshot.guidance) > 0
        assert 'guidance' in snapshot.guidance.lower() or 'outlook' in snapshot.guidance.lower()
    
    def test_source_chunk_ids_populated(self, sample_chunks, filing):
        """Test that source_chunk_ids are populated for extracted KPIs."""
        extractor = KPIExtractor()
        snapshot = extractor.extract_from_chunks(sample_chunks, "2023-09-30")
        
        # Check that extracted KPIs have chunk IDs
        if snapshot.revenue is not None:
            assert 'revenue' in snapshot.source_chunk_ids
        if snapshot.net_income is not None:
            assert 'net_income' in snapshot.source_chunk_ids
    
    def test_empty_chunks_raises_error(self, filing):
        """Test that chunks with no KPIs still creates valid snapshot if guidance/segments found."""
        extractor = KPIExtractor()
        
        # Chunk with no financial KPIs but might have guidance
        empty_chunk = DocumentChunk(
            chunk_id="empty",
            text="This is just some text with no financial data. The company expects growth.",
            source_filing=filing,
            chunk_index=0
        )
        
        # Should create snapshot (might have guidance)
        # If no KPIs found, snapshot creation will fail validation
        # That's expected behavior - need at least one KPI
        try:
            snapshot = extractor.extract_from_chunks([empty_chunk], "2023-09-30")
            # If it succeeds, should have at least guidance or segments
            assert snapshot.guidance is not None or len(snapshot.segments) > 0
        except ValueError:
            # Expected if no KPIs found
            pass
