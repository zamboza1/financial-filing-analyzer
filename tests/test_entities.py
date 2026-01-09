"""Tests for entity classes."""

import pytest
from pathlib import Path
import tempfile
import yaml

from backend.entities import Company, CompanyDirectory, Filing, DocumentChunk, KpiSnapshot


class TestCompany:
    """Test Company class initialization and invariants."""
    
    def test_valid_company_creation(self):
        """Test creating a valid company."""
        company = Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="320193",
            peers=["MSFT", "GOOGL"]
        )
        assert company.ticker == "AAPL"
        assert company.name == "Apple Inc."
        assert company.cik == "0000320193"  # Normalized to 10 digits
        assert company.peers == ["MSFT", "GOOGL"]
    
    def test_ticker_normalization(self):
        """Test that ticker is normalized to uppercase."""
        company = Company(
            ticker="aapl",
            name="Apple Inc.",
            cik="320193",
            peers=[]
        )
        assert company.ticker == "AAPL"
    
    def test_cik_normalization(self):
        """Test that CIK is normalized to 10 digits."""
        # Test with leading zeros
        company1 = Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="0000320193",
            peers=[]
        )
        assert company1.cik == "0000320193"
        
        # Test without leading zeros
        company2 = Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="320193",
            peers=[]
        )
        assert company2.cik == "0000320193"
        
        # Test single digit
        company3 = Company(
            ticker="TEST",
            name="Test Corp",
            cik="5",
            peers=[]
        )
        assert company3.cik == "0000000005"
    
    def test_peers_normalization(self):
        """Test that peers are normalized to uppercase."""
        company = Company(
            ticker="AAPL",
            name="Apple Inc.",
            cik="320193",
            peers=["msft", "  googl  ", "META"]
        )
        assert company.peers == ["MSFT", "GOOGL", "META"]
    
    def test_empty_ticker_raises_error(self):
        """Test that empty ticker raises ValueError."""
        with pytest.raises(ValueError, match="Ticker cannot be empty"):
            Company(ticker="", name="Test", cik="123")
    
    def test_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="Company name cannot be empty"):
            Company(ticker="TEST", name="", cik="123")
    
    def test_invalid_cik_raises_error(self):
        """Test that non-numeric CIK raises ValueError."""
        with pytest.raises(ValueError, match="CIK must be numeric"):
            Company(ticker="TEST", name="Test", cik="abc123")


class TestCompanyDirectory:
    """Test CompanyDirectory class."""
    
    def create_temp_config(self, content: dict) -> Path:
        """Helper to create a temporary YAML config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(content, f)
            return Path(f.name)
    
    def test_load_valid_config(self):
        """Test loading a valid company config."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': ['MSFT', 'GOOGL']
                },
                'MSFT': {
                    'name': 'Microsoft Corporation',
                    'cik': '789019',
                    'peers': ['AAPL']
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            assert len(directory.get_all_tickers()) == 2
            assert 'AAPL' in directory.get_all_tickers()
            assert 'MSFT' in directory.get_all_tickers()
        finally:
            config_path.unlink()
    
    def test_resolve_by_ticker(self):
        """Test resolving company by ticker."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            company = directory.resolve_company("AAPL")
            assert company.ticker == "AAPL"
            assert company.name == "Apple Inc."
        finally:
            config_path.unlink()
    
    def test_resolve_by_ticker_case_insensitive(self):
        """Test that ticker resolution is case-insensitive."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            company = directory.resolve_company("aapl")
            assert company.ticker == "AAPL"
        finally:
            config_path.unlink()
    
    def test_resolve_by_name(self):
        """Test resolving company by exact name match."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            company = directory.resolve_company("Apple Inc.")
            assert company.ticker == "AAPL"
        finally:
            config_path.unlink()
    
    def test_resolve_by_name_case_insensitive(self):
        """Test that name resolution is case-insensitive."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            company = directory.resolve_company("apple inc.")
            assert company.ticker == "AAPL"
        finally:
            config_path.unlink()
    
    def test_resolve_nonexistent_raises_error(self):
        """Test that resolving nonexistent company raises ValueError."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            with pytest.raises(ValueError, match="No company found matching"):
                directory.resolve_company("INVALID")
        finally:
            config_path.unlink()
    
    def test_empty_query_raises_error(self):
        """Test that empty query raises ValueError."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            with pytest.raises(ValueError, match="Query cannot be empty"):
                directory.resolve_company("")
        finally:
            config_path.unlink()
    
    def test_missing_config_file_raises_error(self):
        """Test that missing config file raises FileNotFoundError."""
        fake_path = Path("/nonexistent/path/companies.yaml")
        with pytest.raises(FileNotFoundError):
            CompanyDirectory(fake_path)
    
    def test_invalid_config_format_raises_error(self):
        """Test that invalid config format raises ValueError."""
        config = {'invalid': 'structure'}
        config_path = self.create_temp_config(config)
        
        try:
            with pytest.raises(ValueError, match="missing 'companies' key"):
                CompanyDirectory(config_path)
        finally:
            config_path.unlink()
    
    def test_get_company_returns_none_for_missing(self):
        """Test that get_company returns None for missing ticker."""
        config = {
            'companies': {
                'AAPL': {
                    'name': 'Apple Inc.',
                    'cik': '320193',
                    'peers': []
                }
            }
        }
        config_path = self.create_temp_config(config)
        
        try:
            directory = CompanyDirectory(config_path)
            assert directory.get_company("INVALID") is None
            assert directory.get_company("AAPL") is not None
        finally:
            config_path.unlink()


class TestFiling:
    """Test Filing class."""
    
    def test_valid_filing_creation(self):
        """Test creating a valid filing."""
        company = Company(ticker="AAPL", name="Apple Inc.", cik="320193")
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q"
        )
        assert filing.company.ticker == "AAPL"
        assert filing.accession == "0000320193-23-000077"
        assert filing.filing_type == "10-Q"
    
    def test_empty_accession_raises_error(self):
        """Test that empty accession raises ValueError."""
        company = Company(ticker="AAPL", name="Apple Inc.", cik="320193")
        with pytest.raises(ValueError, match="Accession number cannot be empty"):
            Filing(
                company=company,
                accession="",
                filing_date="2023-11-03",
                period_end="2023-09-30",
                filing_type="10-Q"
            )


class TestDocumentChunk:
    """Test DocumentChunk class."""
    
    def test_valid_chunk_creation(self):
        """Test creating a valid document chunk."""
        company = Company(ticker="AAPL", name="Apple Inc.", cik="320193")
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q"
        )
        chunk = DocumentChunk(
            chunk_id="chunk_001",
            text="This is some filing text.",
            source_filing=filing,
            chunk_index=0
        )
        assert chunk.chunk_id == "chunk_001"
        assert chunk.text == "This is some filing text."
        assert chunk.chunk_index == 0
    
    def test_empty_chunk_id_raises_error(self):
        """Test that empty chunk_id raises ValueError."""
        company = Company(ticker="AAPL", name="Apple Inc.", cik="320193")
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q"
        )
        with pytest.raises(ValueError, match="chunk_id cannot be empty"):
            DocumentChunk(
                chunk_id="",
                text="Some text",
                source_filing=filing,
                chunk_index=0
            )
    
    def test_empty_text_raises_error(self):
        """Test that empty text raises ValueError."""
        company = Company(ticker="AAPL", name="Apple Inc.", cik="320193")
        filing = Filing(
            company=company,
            accession="0000320193-23-000077",
            filing_date="2023-11-03",
            period_end="2023-09-30",
            filing_type="10-Q"
        )
        with pytest.raises(ValueError, match="chunk text cannot be empty"):
            DocumentChunk(
                chunk_id="chunk_001",
                text="",
                source_filing=filing,
                chunk_index=0
            )


class TestKpiSnapshot:
    """Test KpiSnapshot class."""
    
    def test_valid_kpi_snapshot(self):
        """Test creating a valid KPI snapshot."""
        snapshot = KpiSnapshot(
            period_end="2023-09-30",
            revenue=89498.0,
            net_income=22956.0,
            eps=1.46
        )
        assert snapshot.period_end == "2023-09-30"
        assert snapshot.revenue == 89498.0
        assert snapshot.net_income == 22956.0
        assert snapshot.eps == 1.46
    
    def test_empty_snapshot_raises_error(self):
        """Test that snapshot with no KPIs raises ValueError."""
        with pytest.raises(ValueError, match="must have at least one KPI field populated"):
            KpiSnapshot(period_end="2023-09-30")
    
    def test_to_dict_serialization(self):
        """Test that to_dict produces valid dictionary."""
        snapshot = KpiSnapshot(
            period_end="2023-09-30",
            revenue=89498.0,
            net_income=22956.0,
            source_chunk_ids={"revenue": "chunk_001"}
        )
        data = snapshot.to_dict()
        assert data['period_end'] == "2023-09-30"
        assert data['revenue'] == 89498.0
        assert data['net_income'] == 22956.0
        assert data['source_chunk_ids']['revenue'] == "chunk_001"
        assert data['segments'] == []  # Default empty list
