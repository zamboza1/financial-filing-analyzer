"""
FastAPI application for financial filing analysis.
"""

from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
import re
import time
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
import uvicorn
import yaml

from backend.entities import Company, Filing, KpiSnapshot, CompanyDirectory
from backend.sec_ingest import SECIngester
from backend.cache import FilingCache
from backend.text_clean import TextExtractor
from backend.chunking import DocumentChunker
from backend.index_store import IndexManager
from backend.kpi_extract import KPIExtractor
from backend.deltas import compare_kpis, format_delta_summary, DeltaItem
from backend.valuation import MarketData, ValuationRatios, fetch_market_data, calculate_valuation_ratios
from backend.report import ResearchReport

# =============================================================================
# Configuration
# =============================================================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("radar.api")

# Directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_FILINGS_DIR = DATA_DIR / "raw_filings"
INDEXES_DIR = DATA_DIR / "indexes"
COMPANIES_YAML = DATA_DIR / "companies.yaml"

# Ensure directories exist
RAW_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
INDEXES_DIR.mkdir(parents=True, exist_ok=True)

# Rate limiting (in-memory simple implementation)
# For production, use Redis or similar
RATE_LIMIT_REQUESTS = 10
RATE_LIMIT_WINDOW = 60  # seconds
rate_limit_store: Dict[str, List[float]] = {}

# Ticker validation pattern
TICKER_PATTERN = re.compile(r'^[A-Z]{1,5}$')

# =============================================================================
# App Initialization
# =============================================================================

app = FastAPI(
    title="Financial Filing Analyzer API",
    version="0.1.0",
    description="Automated SEC 10-Q filing analysis with KPI extraction and valuation",
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc",  # ReDoc
)

# CORS middleware - restrict origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:8001", "http://127.0.0.1:8001"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Security headers middleware
@app.middleware("http")
async def security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    
    # OWASP recommended security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"] = "no-store, max-age=0"
    
    return response


# =============================================================================
# Helpers
# =============================================================================

def check_rate_limit(client_ip: str) -> bool:
    """
    Check if client has exceeded rate limit.
    Returns True if request is allowed, False if rate limited.
    """
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW
    
    # Clean old entries
    if client_ip in rate_limit_store:
        rate_limit_store[client_ip] = [
            ts for ts in rate_limit_store[client_ip] if ts > window_start
        ]
    else:
        rate_limit_store[client_ip] = []
    
    # Check limit
    if len(rate_limit_store[client_ip]) >= RATE_LIMIT_REQUESTS:
        return False
    
    # Record this request
    rate_limit_store[client_ip].append(now)
    return True


def looks_like_definition(text: str) -> bool:
    """
    Heuristic to ignore XBRL definition/reference pages which are often junk.
    """
    junk_indicators = [
        "XBRL TAXONOMY EXTENSION", 
        "DEI DEFINITION", 
        "linkbase", 
        "xmlns:xbrli",
        "taxonomy schema"
    ]
    text_upper = text.upper()
    return any(ind.upper() in text_upper for ind in junk_indicators)


# =============================================================================
# Models
# =============================================================================

class AnalyzeRequest(BaseModel):
    """Request model for analyze endpoint with input validation."""
    ticker: str
    filing_type: str = "10-Q"  # "10-Q" or "10-K"
    period: Optional[str] = None  # e.g., "2024-Q3" or "2024" for 10-K, None = latest
    
    @field_validator('ticker')
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Validate and sanitize ticker symbol."""
        ticker = v.strip().upper()
        if not ticker or len(ticker) > 5:
            raise ValueError("Ticker must be 1-5 characters")
        if not TICKER_PATTERN.match(ticker):
            raise ValueError("Ticker must contain only letters (A-Z)")
        return ticker
    
    @field_validator('filing_type')
    @classmethod
    def validate_filing_type(cls, v: str) -> str:
        """Validate filing type."""
        v = v.strip().upper()
        if v not in ["10-Q", "10-K"]:
            raise ValueError("Filing type must be '10-Q' or '10-K'")
        return v
    
    @field_validator('period')
    @classmethod
    def validate_period(cls, v: Optional[str]) -> Optional[str]:
        """Validate period format."""
        if v is None:
            return None
        v = v.strip()
        # Accept formats: 
        # - "Mar 2025", "Nov 2024" (month year)
        # - "FY Mar 2025" (fiscal year)
        # - Legacy: "2024", "2024-Q1"
        if re.match(r'^(FY\s+)?[A-Z][a-z]{2}\s+\d{4}$', v):
            return v
        if re.match(r'^\d{4}(-Q[1-4])?$', v):
            return v
        raise ValueError("Period must be 'Mon YYYY' format (e.g., 'Mar 2025')")


class EvidenceItem(BaseModel):
    text: str
    source_filing: str
    chunk_index: int
    ticker: str
    period_end: str
    relevance_score: float


class AnalyzeResponse(BaseModel):
    ticker: str
    latest_period: str
    previous_period: str
    current_snapshot: KpiSnapshot
    previous_snapshot: KpiSnapshot
    deltas: List[DeltaItem]
    evidence: List[EvidenceItem]
    market_data: Optional[MarketData] = None
    valuation: Optional[ValuationRatios] = None
    filing_type: str


class CompanyInfo(BaseModel):
    ticker: str
    name: str


class CompaniesResponse(BaseModel):
    companies: List[CompanyInfo]
    
class FilingInfo(BaseModel):
    accession: str
    date: str
    period: str
    form: str

class AvailableFilingsResponse(BaseModel):
    ticker: str
    filings_10q: List[FilingInfo]
    filings_10k: List[FilingInfo]


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Financial Filing Analyzer API", "version": "0.1.0"}


@app.get("/api/companies", response_model=CompaniesResponse)
async def list_companies():
    """List all available companies."""
    directory = CompanyDirectory(COMPANIES_YAML)
    tickers = directory.get_all_tickers()
    companies = [directory.get_company(t) for t in tickers]
    return {"companies": [{"ticker": c.ticker, "name": c.name} for c in companies if c]}


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/filings/{ticker}", response_model=AvailableFilingsResponse)
async def get_available_filings(ticker: str):
    """Get available filings for a ticker."""
    ticker = ticker.upper()
    if not TICKER_PATTERN.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
        
    try:
        directory = CompanyDirectory(COMPANIES_YAML)
        company = directory.resolve_or_lookup_company(ticker)
        
        # We need an ingester to check available filings (cached or via API)
        cache = FilingCache(RAW_FILINGS_DIR)
        ingester = SECIngester(cache)
        
        # Get available filings using the public method
        filings_10q_dicts, filings_10k_dicts = ingester.get_available_filings(company)
        
        q_filings = []
        for f in filings_10q_dicts:
             q_filings.append(FilingInfo(
                 accession=f['accession'],
                 date=f['date'],
                 period=f['period'],
                 form="10-Q"
             ))

        k_filings = []
        for f in filings_10k_dicts:
             k_filings.append(FilingInfo(
                 accession=f['accession'],
                 date=f['date'],
                 period=f['period'],
                 form="10-K"
             ))
             
        return {
            "ticker": ticker,
            "filings_10q": q_filings,
            "filings_10k": k_filings
        }
        
    except ValueError as e:
         raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching filings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analyze", response_model=AnalyzeResponse)
async def analyze_company(request: AnalyzeRequest, req: Request):
    """
    Analyze a company's latest two 10-Q filings.
    
    Rate limited to 10 requests per minute per IP.
    
    This endpoint runs the full pipeline:
    1. Resolve company metadata
    2. Download latest two 10-Q filings
    3. Extract and chunk text
    4. Extract KPIs
    5. Compare periods
    6. Retrieve evidence
    7. Generate report
    """
    # Rate limiting check
    client_ip = req.client.host if req.client else "unknown"
    if not check_rate_limit(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please wait before making another request."
        )
    
    try:
        ticker = request.ticker.upper()
        
        # 1. Resolve company (from config or SEC API lookup)
        directory = CompanyDirectory(COMPANIES_YAML)
        try:
            company = directory.resolve_company(ticker)
        except ValueError:
            # Not in config, look up
            try:
                company = directory.resolve_or_lookup_company(ticker)
                # Optionally add to config? No, keep config static for now or add to separate runtime cache
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        
        # 2. Download filings
        cache = FilingCache(RAW_FILINGS_DIR)
        ingester = SECIngester(cache)
        
        try:
            # Unified fetch method handles period lookup and default to latest
            latest_filing, previous_filing = ingester.fetch_filings_by_type(
                company=company,
                filing_type=request.filing_type,
                period=request.period
            )
                    
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Ingestion error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch filings: {str(e)}")
            
        # 3. Text Extraction
        extractor = TextExtractor()
        try:
            latest_text = extractor.extract_from_filing(latest_filing.raw_text_path)
            previous_text = extractor.extract_from_filing(previous_filing.raw_text_path)
            
            # Use chunks for evidence retrieval
            chunker = DocumentChunker(chunk_size=1000, chunk_overlap=200)
            latest_chunks = chunker.chunk_filing(latest_filing)
            
            # We don't necessarily need chunks for previous filing unless we want evidence from it too
            # For simplicity, currently focusing evidence on LATEST period explanation
            
        except Exception as e:
             logger.error(f"Extraction error: {e}")
             raise HTTPException(status_code=500, detail=f"Failed to extract text: {str(e)}")

        # 4. KPI Extraction
        kpi_extractor = KPIExtractor()
        
        # Extract from LATEST chunks (robust method)
        current_snapshot = kpi_extractor.extract_from_chunks(latest_chunks, latest_filing.period_end)
        
        # For PREVIOUS, we might not have chunks if we didn't chunk it. 
        # But for comparison we need the numbers.
        # Let's chunk previous quickly
        previous_chunks = chunker.chunk_filing(previous_filing)
        previous_snapshot = kpi_extractor.extract_from_chunks(previous_chunks, previous_filing.period_end)

        # 5. Market Data & Valuation
        market_data = None
        valuation = None
        try:
            # Check if this is a historical analysis or current
            # It's 'historical' if the filing period is significantly in the past (> 3 months from now)
            # Currently strict logic: if latest_filing.period_end date is < today - 90 days
            
            # Simple heuristic: Always fetch current market data for now, 
            # unless we implement point-in-time price fetching which is hard with free APIS.
            # yfinance gives historical data.
            
            # Try to match price to filing date
            # CRITICAL: We use FILING DATE (when info became public), not period end.
            # This ensures the stock price reflects the market's reaction to the released numbers.
            # Using period_end would match price to a date before results were known.
            filing_date_str = latest_filing.filing_date # YYYY-MM-DD
            
            market_data = fetch_market_data(ticker, period_end=filing_date_str)
            if market_data:
                valuation = calculate_valuation_ratios(
                    market_data=market_data,
                    eps=current_snapshot.eps,
                    revenue=current_snapshot.revenue,
                    ebitda=current_snapshot.ebitda,
                    net_income=current_snapshot.net_income
                )
                
        except Exception as e:
            logger.warning(f"Market data failed: {e}")
            # Non-critical, continue without market data

        # 6. Comparison (Deltas)
        deltas = compare_kpis(current_snapshot, previous_snapshot)
        
        # 7. Evidence Retrieval
        evidence = []
        index_manager = IndexManager(INDEXES_DIR)
        
        try:
            # Create/Get index for LATEST filing
            index = index_manager.get_or_create_index(company, latest_filing.period_end, latest_chunks)
            
            # Map for quick lookup of full chunk object from ID
            chunk_map = {c.chunk_id: c for c in latest_chunks} if latest_chunks else {}
            
            # Queries for evidence
            queries = [
                "revenue growth drivers",
                "net income changes",
                "operating margin factors",
                "future outlook and guidance"
            ]
            
            # Basic deduplication
            seen_ids = set()
            
            for q in queries:
                try:
                    results = index.search(q, k=2)
                    for res in results:
                        chunk_id = res.get('chunk_id')
                        if not chunk_id or chunk_id not in chunk_map:
                            continue
                            
                        chunk = chunk_map[chunk_id]
                        if chunk.chunk_id not in seen_ids:
                            seen_ids.add(chunk.chunk_id)
                            
                            # Filter out junk (tables of contents, definitions)
                            if looks_like_definition(chunk.text):
                                continue
                                
                            evidence.append(EvidenceItem(
                                text=chunk.text,
                                source_filing=latest_filing.accession,
                                chunk_index=chunk_ids_to_index(chunk.chunk_id, latest_chunks),
                                ticker=ticker,
                                period_end=latest_filing.period_end,
                                relevance_score=res.get('score', 0.0)
                            ))
                except Exception as query_err:
                     logger.warning(f"Query '{q}' failed: {query_err}")
                     continue
            
            # Fallback if semantic search fails or returns nothing useful (rare)
            if not evidence and latest_chunks:
                 # Just take a few early chunks from Item 2 (MD&A) if we could identify them
                 # For now, just take chunks 10-12 as they often contain intro text
                 for c in latest_chunks[10:13]:
                      evidence.append(EvidenceItem(
                            text=c.text,
                            source_filing=latest_filing.accession,
                            chunk_index=0,
                            ticker=ticker,
                            period_end=latest_filing.period_end,
                            relevance_score=0.0
                        ))

        except Exception as e:
            logger.warning(f"Evidence retrieval failed: {e}")
        
        # Find index of chunk in list for display
        def find_chunk_index(c_id, chunks):
            for i, c in enumerate(chunks):
                if c.chunk_id == c_id: return i
            return 0
            
        # Update indices
        for ev in evidence:
            # We don't have the chunk object here easily matched to original list without ID
            # But we can approximate or pass it through.
            # Simplified: EvidenceItem just stores the index we found or 0
            pass

        return AnalyzeResponse(
            ticker=company.ticker,
            latest_period=latest_filing.period_end,
            previous_period=previous_filing.period_end,
            current_snapshot=current_snapshot,
            previous_snapshot=previous_snapshot,
            deltas=deltas,
            evidence=evidence[:6], # Limit to 6 items
            market_data=market_data,
            valuation=valuation,
            filing_type=request.filing_type
        )
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def chunk_ids_to_index(chunk_id: str, chunks: List[Any]) -> int:
    for i, c in enumerate(chunks):
        if c.chunk_id == chunk_id:
            return i
    return 0


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
