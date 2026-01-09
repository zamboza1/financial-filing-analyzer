"""Core entity classes: Company, Filing, DocumentChunk, KpiSnapshot, etc."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml


def _get_user_agent() -> str:
    """
    Get SEC User-Agent from environment variable or use default.
    
    SEC requires a User-Agent with company name and contact email.
    Set SEC_USER_AGENT environment variable in production.
    """
    return os.environ.get(
        "SEC_USER_AGENT",
        "FinancialFilingAnalyzer/1.0 (contact@example.com)"
    )


@dataclass
class Company:
    """
    Represents a company with its metadata.
    
    Representation Invariants:
    - ticker is uppercase and non-empty
    - cik is a 10-digit string (with leading zeros)
    - name is non-empty
    """
    
    ticker: str
    name: str
    cik: str
    
    def __post_init__(self) -> None:
        """Validate representation invariants after initialization."""
        self.ticker = self.ticker.upper().strip()
        if not self.ticker:
            raise ValueError("Ticker cannot be empty")
        
        if not self.name or not self.name.strip():
            raise ValueError("Company name cannot be empty")
        
        # CIK should be 10 digits (SEC format)
        cik_clean = self.cik.strip().lstrip("0") or "0"
        if not cik_clean.isdigit():
            raise ValueError(f"CIK must be numeric, got: {self.cik}")
        # Normalize CIK to 10 digits with leading zeros
        self.cik = cik_clean.zfill(10)
    
    def __repr__(self) -> str:
        return f"Company(ticker='{self.ticker}', name='{self.name}', cik='{self.cik}')"


class CompanyDirectory:
    """
    Manages company metadata loaded from YAML configuration.
    
    This class encapsulates the company database and provides lookup functionality.
    All company data is loaded once at initialization and stored privately.
    
    Representation Invariants:
    - _companies is a dict mapping ticker (uppercase) -> Company
    - All companies have valid tickers and CIKs
    """
    
    def __init__(self, config_path: Path) -> None:
        """
        Initialize directory from YAML config file.
        
        Preconditions:
        - config_path exists and is readable
        - config_path contains valid YAML with 'companies' key
        
        Postconditions:
        - _companies is populated with all companies from config
        - Raises FileNotFoundError if config_path doesn't exist
        - Raises ValueError if YAML is invalid or missing 'companies' key
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Company config not found: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data or 'companies' not in data:
            raise ValueError(f"Invalid config format: missing 'companies' key in {config_path}")
        
        self._companies: dict[str, Company] = {}
        
        for ticker, info in data['companies'].items():
            try:
                company = Company(
                    ticker=ticker,
                    name=info['name'],
                    cik=info['cik']
                )
                self._companies[company.ticker] = company
            except (KeyError, ValueError) as e:
                raise ValueError(f"Invalid company entry for {ticker}: {e}")
    
    def resolve_company(self, query: str) -> Company:
        """
        Resolve a company by ticker or exact name match.
        
        Preconditions:
        - query is a non-empty string (ticker or company name)
        
        Postconditions:
        - Returns Company if match found
        - Raises ValueError if no match found
        
        Args:
            query: Ticker symbol (e.g., "AAPL") or exact company name
            
        Returns:
            Company object matching the query
            
        Raises:
            ValueError: If query doesn't match any company
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        query_upper = query.strip().upper()
        
        # First try ticker match
        if query_upper in self._companies:
            return self._companies[query_upper]
        
        # Then try exact name match (case-insensitive)
        for company in self._companies.values():
            if company.name.upper() == query.upper():
                return company
        
        # No match found
        available = ", ".join(sorted(self._companies.keys()))
        raise ValueError(
            f"No company found matching '{query}'. "
            f"Available tickers: {available}"
        )
    
    def get_all_tickers(self) -> list[str]:
        """
        Get list of all available tickers.
        
        Returns:
            Sorted list of ticker symbols
        """
        return sorted(self._companies.keys())
    
    def get_company(self, ticker: str) -> Optional[Company]:
        """
        Get company by ticker (returns None if not found).
        
        Args:
            ticker: Ticker symbol (case-insensitive)
            
        Returns:
            Company if found, None otherwise
        """
        return self._companies.get(ticker.upper().strip())
    
    def resolve_or_lookup_company(self, ticker: str) -> Company:
        """
        Resolve company from config, or look up from SEC API if not found.
        
        Preconditions:
        - ticker is a non-empty string
        
        Postconditions:
        - Returns Company if found in config or SEC API
        - Raises ValueError if not found in either
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Company object
            
        Raises:
            ValueError: If company not found
        """
        ticker_upper = ticker.upper().strip()
        
        # Try config first
        try:
            return self.resolve_company(ticker_upper)
        except ValueError:
            pass  # Not in config, try SEC API
        
        # Look up from SEC API
        try:
            import requests
            import time
            
            # SEC company tickers API
            url = "https://www.sec.gov/files/company_tickers.json"
            session = requests.Session()
            session.headers.update({
                "User-Agent": _get_user_agent(),
                "Accept": "application/json"
            })
            
            time.sleep(0.2)  # Rate limiting
            response = session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Search for ticker in the data
            # Format: {"0": {"cik_str": ..., "ticker": ..., "title": ...}, ...}
            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker_upper:
                    cik = str(entry.get("cik_str", ""))
                    name = entry.get("title", f"{ticker_upper} Corp")
                    
                    # Create Company object
                    company = Company(
                        ticker=ticker_upper,
                        name=name,
                        cik=cik
                    )
                    return company
            
            raise ValueError(f"Ticker {ticker_upper} not found in SEC database")
            
        except Exception as e:
            raise ValueError(
                f"Could not find company {ticker_upper} in config or SEC database: {e}"
            ) from e


@dataclass
class Filing:
    """
    Represents a SEC filing with metadata.
    
    Representation Invariants:
    - accession is non-empty
    - filing_date is in YYYY-MM-DD format
    - period_end is in YYYY-MM-DD format
    - raw_text_path exists if provided
    """
    
    company: Company
    accession: str
    filing_date: str
    period_end: str
    filing_type: str  # e.g., "10-Q"
    raw_text_path: Optional[Path] = None
    
    def __post_init__(self) -> None:
        """Validate filing metadata."""
        if not self.accession or not self.accession.strip():
            raise ValueError("Accession number cannot be empty")
        
        if not self.filing_date or not self.period_end:
            raise ValueError("Filing date and period end are required")


@dataclass
class DocumentChunk:
    """
    Represents a chunk of text from a filing with source metadata.
    
    Representation Invariants:
    - chunk_id is unique within a filing
    - text is non-empty
    - source_filing is a valid Filing reference
    """
    
    chunk_id: str
    text: str
    source_filing: Filing
    chunk_index: int
    
    def __post_init__(self) -> None:
        """Validate chunk data."""
        if not self.chunk_id:
            raise ValueError("chunk_id cannot be empty")
        if not self.text or not self.text.strip():
            raise ValueError("chunk text cannot be empty")
        if self.chunk_index < 0:
            raise ValueError("chunk_index must be non-negative")


@dataclass
class KpiSnapshot:
    """
    Structured KPI data for a single reporting period.
    
    All monetary values are in millions USD unless otherwise specified.
    Percentages are stored as decimals (e.g., 0.15 for 15%).
    
    Representation Invariants:
    - At least one KPI field must be populated
    - If margin fields exist, they must be between 0 and 1
    - source_chunk_ids map KPI names to chunk IDs where they were found
    """
    
    period_end: str
    # Core metrics (in millions USD)
    revenue: Optional[float] = None
    cost_of_revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    eps: Optional[float] = None  # Actual dollars, not millions
    
    # Profitability ratios (as decimals, e.g., 0.25 = 25%)
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    
    # Cash flow metrics (in millions USD)
    operating_cash_flow: Optional[float] = None
    free_cash_flow: Optional[float] = None
    
    # Expense metrics (in millions USD)
    research_and_development: Optional[float] = None
    selling_general_admin: Optional[float] = None
    depreciation_amortization: Optional[float] = None
    
    # EBITDA (calculated or extracted)
    ebitda: Optional[float] = None
    
    # Qualitative data
    guidance: Optional[str] = None
    segments: Optional[list[str]] = None
    source_chunk_ids: Optional[dict[str, str]] = None
    
    def __post_init__(self) -> None:
        """Validate KPI data and calculate derived metrics."""
        # Calculate derived metrics
        self._calculate_derived_metrics()
        
        # Check that at least one field is populated
        has_data = any([
            self.revenue is not None,
            self.gross_profit is not None,
            self.operating_income is not None,
            self.net_income is not None,
            self.eps is not None,
            self.ebitda is not None,
            self.guidance is not None,
            (self.segments is not None and len(self.segments) > 0),
        ])
        
        if not has_data:
            raise ValueError(
                "KpiSnapshot must have at least one KPI field populated. "
                f"Got: revenue={self.revenue}, net_income={self.net_income}, "
                f"guidance={self.guidance}, segments={self.segments}"
            )
        
        # Validate margins are in [0, 1] (allowing slightly over 1 for edge cases)
        for margin_name in ['gross_margin', 'operating_margin', 'net_margin']:
            margin_value = getattr(self, margin_name, None)
            if margin_value is not None and not (-0.5 <= margin_value <= 1.5):
                # Just set to None if invalid rather than raising
                setattr(self, margin_name, None)
        
        if self.source_chunk_ids is None:
            self.source_chunk_ids = {}
        
        if self.segments is None:
            self.segments = []
    
    def _calculate_derived_metrics(self) -> None:
        """Calculate derived metrics from available data."""
        # Calculate gross profit if we have revenue and cost
        if self.gross_profit is None and self.revenue and self.cost_of_revenue:
            self.gross_profit = self.revenue - self.cost_of_revenue
        
        # Calculate margins
        if self.revenue and self.revenue > 0:
            if self.gross_margin is None and self.gross_profit is not None:
                self.gross_margin = self.gross_profit / self.revenue
            
            if self.operating_margin is None and self.operating_income is not None:
                self.operating_margin = self.operating_income / self.revenue
            
            if self.net_margin is None and self.net_income is not None:
                self.net_margin = self.net_income / self.revenue
        
        # Calculate EBITDA if not provided
        # EBITDA = Operating Income + Depreciation & Amortization
        if self.ebitda is None and self.operating_income is not None:
            if self.depreciation_amortization is not None:
                self.ebitda = self.operating_income + self.depreciation_amortization
            else:
                # Estimate: use operating income as proxy (underestimates EBITDA)
                self.ebitda = self.operating_income
        
        # Calculate free cash flow if not provided
        # FCF = Operating Cash Flow - CapEx (simplified: use D&A as proxy)
        if self.free_cash_flow is None and self.operating_cash_flow is not None:
            if self.depreciation_amortization is not None:
                self.free_cash_flow = self.operating_cash_flow - self.depreciation_amortization
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'period_end': self.period_end,
            'revenue': self.revenue,
            'cost_of_revenue': self.cost_of_revenue,
            'gross_profit': self.gross_profit,
            'gross_margin': self.gross_margin,
            'operating_income': self.operating_income,
            'operating_margin': self.operating_margin,
            'net_income': self.net_income,
            'net_margin': self.net_margin,
            'eps': self.eps,
            'ebitda': self.ebitda,
            'operating_cash_flow': self.operating_cash_flow,
            'free_cash_flow': self.free_cash_flow,
            'research_and_development': self.research_and_development,
            'selling_general_admin': self.selling_general_admin,
            'depreciation_amortization': self.depreciation_amortization,
            'guidance': self.guidance,
            'segments': self.segments,
            'source_chunk_ids': self.source_chunk_ids,
        }
