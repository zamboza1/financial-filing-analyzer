"""SEC filing download and extraction."""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.entities import Company, Filing
from backend.cache import FilingCache


def _get_default_user_agent() -> str:
    """Get SEC User-Agent from environment variable or use default."""
    return os.environ.get(
        "SEC_USER_AGENT",
        "FinancialFilingAnalyzer/1.0 (contact@example.com)"
    )


class SECIngester:
    """
    Handles downloading and processing SEC filings.
    
    This class encapsulates SEC EDGAR API interactions and filing retrieval.
    It respects SEC rate limits and uses caching to avoid redundant downloads.
    
    Representation Invariants:
    - cache is a valid FilingCache instance
    - session has proper headers set for SEC API compliance
    """
    
    # SEC API base URLs
    SUBMISSIONS_API = "https://data.sec.gov/submissions"
    ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
    
    def __init__(self, cache: FilingCache, user_agent: str = None) -> None:
        """
        Initialize SEC ingester with cache.
        
        Preconditions:
        - cache is a valid FilingCache instance
        
        Postconditions:
        - session is configured with proper headers and retry strategy
        - _cache is set
        - _user_agent is stored for reuse
        
        Args:
            cache: FilingCache instance for managing downloads
            user_agent: Optional user agent string (defaults to SEC-compliant format)
        """
        self._cache = cache
        
        # SEC requires a User-Agent header with company/contact info
        # Format: CompanyName/Version (ContactInfo)
        # Set SEC_USER_AGENT environment variable in production
        if user_agent is None:
            user_agent = _get_default_user_agent()
        
        # Store user agent for reuse in archive requests
        self._user_agent = user_agent
        
        self._session = requests.Session()
        # DO NOT set Host header - requests library sets it automatically based on URL
        self._session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json"
        })
        
        # Add retry strategy for network issues
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
    
    def _get_company_submissions(self, company: Company) -> dict:
        """
        Fetch company submissions from SEC API.
        
        Preconditions:
        - company has valid CIK
        
        Postconditions:
        - Returns submissions JSON dict
        - Raises requests.RequestException on network errors
        - Raises ValueError if response is invalid
        
        Args:
            company: Company entity with CIK
            
        Returns:
            Submissions JSON as dictionary
            
        Raises:
            requests.RequestException: On network or HTTP errors
            ValueError: If response is invalid
        """
        url = f"{self.SUBMISSIONS_API}/CIK{company.cik}.json"
        
        try:
            # Add delay to respect SEC rate limits (300ms = max 3.3 requests/second, well under 10/sec limit)
            time.sleep(0.3)
            
            response = self._session.get(url, timeout=10)
            response.raise_for_status()
            
            # Check for HTTP error status codes first
            if response.status_code == 403:
                raise requests.RequestException(
                    f"SEC returned 403 Forbidden. User-Agent: {self._user_agent}"
                )
            
            # Try to parse JSON first - if it's valid JSON with expected structure, return it immediately
            # This prevents false positives from blocking detection
            try:
                data = response.json()
                # Check if it's valid submissions data (has 'cik' field or 'filings' structure)
                if isinstance(data, dict):
                    # Valid submissions JSON should have either 'cik' or 'filings' key
                    if 'cik' in data or 'filings' in data:
                        return data  # Success! Return immediately
                    # If it's a dict but doesn't have expected structure, might be an error
                    if 'error' in data or 'message' in data:
                        raise requests.RequestException(
                            f"SEC API returned error: {data.get('error', data.get('message', 'Unknown error'))}"
                        )
            except (ValueError, json.JSONDecodeError):
                # JSON parsing failed - might be an HTML error page or blocking message
                pass
            
            # Only check for blocking if JSON parsing failed
            response_text = response.text
            
            # Check for empty response
            if not response_text or len(response_text) < 100:
                raise requests.RequestException(
                    f"SEC returned empty or suspiciously short response. "
                    f"Status: {response.status_code}, Length: {len(response_text)}"
                )
            
            # Check if SEC blocked us (multiple indicators)
            blocking_indicators = [
                "Undeclared Automated Tool",
                "Your Request Originates",
                "Access Denied",
                "Forbidden"
            ]
            if any(indicator in response_text for indicator in blocking_indicators):
                raise requests.RequestException(
                    f"SEC blocked request - User-Agent may need updating. "
                    f"Status: {response.status_code}, Response preview: {response_text[:200]}"
                )
            
            # If we get here, response is not valid JSON and not a known blocking message
            raise requests.RequestException(
                f"Unexpected response format from SEC API. "
                f"Status: {response.status_code}, Content preview: {response_text[:200]}"
            )
            
        except requests.RequestException as e:
            raise requests.RequestException(
                f"Failed to fetch submissions for {company.ticker} (CIK: {company.cik}): {e}"
            ) from e
    
    def _calculate_document_priority(self, href: str, description: str) -> int:
        """
        Calculate priority for document links (lower = higher priority).
        FIXED: Less aggressive XBRL filtering - accepts XBRL-enhanced HTML
        
        Args:
            href: Document URL
            description: Description text
            
        Returns:
            Priority score (lower is better)
        """
        href_lower = href.lower()
        desc_lower = description.lower()
        combined = (href_lower + ' ' + desc_lower).lower()
        
        # ONLY skip pure XBRL instance documents (XML files)
        # Don't skip XBRL-enhanced HTML - modern filings use this format
        if href_lower.endswith('.xml') and 'instance' in combined:
            return 99  # Skip pure XBRL XML instance documents
        
        # Highest priority: .txt files (complete submission text)
        if '.txt' in href_lower and 'exhibit' not in desc_lower:
            return 1
        
        # High priority: HTML files that mention 10-Q
        if ('.htm' in href_lower or '.html' in href_lower):
            if 'exhibit' not in desc_lower:
                if '10q' in href_lower or '10-q' in href_lower:
                    return 2
                # Accept files with patterns like "d123456d10q.htm"
                if re.search(r'd\d+.*10q', href_lower):
                    return 2
                return 3
        
        # Lower priority: 10-K (fallback)
        if '10-k' in combined or '10k' in combined:
            return 5
        
        # Low priority: exhibits
        if 'exhibit' in desc_lower:
            return 10
        
        return 6
    
    def _is_valid_filing_content(self, content: bytes) -> tuple[bool, str]:
        """
        Validate that downloaded content is actual filing content.
        Returns (is_valid, reason)
        
        FIXED: Accepts complete submission files early, rejects pure XBRL metadata
        
        Args:
            content: Downloaded content as bytes
            
        Returns:
            Tuple of (is_valid: bool, reason: str)
        """
        try:
            head = content.decode('utf-8', errors='ignore')[:20000].lower()
        except:
            return False, "Could not decode content"
        
        # Accept very large files early (don't let header-only heuristics reject them)
        if len(content) >= 200000:
            return True, "Very large file, likely valid"
        
        # Accept SEC complete submission files early; TextExtractor will pull the main 10-Q out of them
        if "<sec-document>" in head or ("<document>" in head and "</document>" in head):
            if len(content) >= 50000:
                return True, "SEC submission file"
        
        text = head
        
        # Check for blocking
        blocking_indicators = [
            "undeclared automated tool",
            "your request originates",
            "access denied",
        ]
        if any(indicator in text for indicator in blocking_indicators):
            return False, "SEC blocked request"
        
        # STRICT: Reject pure XBRL metadata files
        # These have lots of XBRL metadata but no actual financial statements
        xbrl_metadata_indicators = [
            'entity information [line items]',
            'period type:',
            'definitionboolean flag',
            'namespace prefix:',
            'data type:',
            'balance type:',
            'entity central index key',
            'entity registrant name',
            'entity address',
        ]
        xbrl_metadata_count = sum(1 for ind in xbrl_metadata_indicators if ind in text)
        
        # Check for actual financial statement content (not just metadata)
        financial_statement_indicators = [
            'consolidated statements of operations',
            'consolidated statements of income',
            'income statement',
            'statement of operations',
            'revenues',
            'net sales',
            'cost of sales',
            'gross profit',
            'operating expenses',
            'operating income',
            'income before income taxes',
            'provision for income taxes',
            'net income',
            'earnings per share',
            'basic',
            'diluted',
        ]
        financial_statement_count = sum(1 for ind in financial_statement_indicators if ind in text)
        
        # Also check for Item sections (actual 10-Q content)
        item_indicators = [
            'item 1.',
            'item 2.',
            'item 3.',
            'part i',
            'part ii',
        ]
        item_count = sum(1 for ind in item_indicators if ind in text)
        
        # REJECT if it's clearly XBRL metadata without financial statements
        if xbrl_metadata_count >= 4 and financial_statement_count == 0 and item_count == 0:
            return False, f"Pure XBRL metadata file ({xbrl_metadata_count} metadata indicators, no financial statements)"
        
        # Check if it's a pure index/directory page
        index_indicators = [
            'directory list of',
            'quick edgar tutorial',
            'company filings search',
        ]
        index_count = sum(1 for ind in index_indicators if ind in text)
        
        if index_count >= 2 and financial_statement_count == 0 and item_count == 0:
            return False, "Index page without filing content"
        
        # ACCEPT if it has financial statement content or Item sections
        if financial_statement_count >= 3:
            return True, f"Valid filing with financial statements ({financial_statement_count} indicators)"
        
        if item_count >= 2:
            return True, f"Valid filing with Item sections ({item_count} indicators)"
        
        # ACCEPT if it has some financial content and is large enough
        if financial_statement_count >= 1 and len(content) >= 100000:
            return True, f"Large file with financial content ({financial_statement_count} indicators)"
        
        # Check minimum size
        if len(content) < 20000:
            return False, f"File too small ({len(content)} bytes)"
        
        # If size is very large, likely valid even if indicators are unclear
        if len(content) >= 200000:
            return True, "Very large file, likely valid"
        
        return False, f"Insufficient financial statement content (found {financial_statement_count} financial indicators, {item_count} item indicators)"
    
    def _is_index_page(self, text: str) -> bool:
        """
        Check if text appears to be an SEC index page rather than filing content.
        
        Args:
            text: Text content to check
            
        Returns:
            True if appears to be index page
        """
        text_lower = text.lower()
        index_indicators = [
            'directory list of',
            'search options',
            'skip to main content',
            'quick edgar tutorial',
            'company filings search',
            'site map',
            'accessibility',
            'privacy',
            'inspector general'
        ]
        
        # Count how many index indicators are present
        indicator_count = sum(1 for indicator in index_indicators if indicator in text_lower)
        
        # If 3+ indicators, it's probably an index page
        if indicator_count >= 3:
            return True
        
        # Also check if it lacks filing content indicators
        filing_indicators = ['item 1', 'item 2', 'financial statements', 'consolidated', 'balance sheet']
        has_filing_content = any(indicator in text_lower for indicator in filing_indicators)
        
        # If it has index indicators but no filing content, it's an index page
        if indicator_count >= 2 and not has_filing_content:
            return True
        
        return False
    
    def _parse_filing_date(self, date_str: str) -> str:
        """
        Parse SEC date format to YYYY-MM-DD.
        
        SEC dates are typically YYYYMMDD format.
        
        Args:
            date_str: Date string from SEC (various formats)
            
        Returns:
            Date in YYYY-MM-DD format
        """
        # Try YYYYMMDD format
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # Try YYYY-MM-DD format (already correct)
        if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str
        
        # Fallback: try to parse
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return date_str  # Return as-is if can't parse
    
    def _get_latest_10q_filings(self, company: Company, limit: int = 2) -> list[dict]:
        """
        Get latest 10-Q filings for a company.
        
        Preconditions:
        - company has valid CIK
        - limit > 0
        
        Postconditions:
        - Returns list of filing metadata dicts, sorted by date (newest first)
        - Returns at most 'limit' filings
        - Only includes 10-Q form type
        
        Args:
            company: Company entity
            limit: Maximum number of filings to return
            
        Returns:
            List of filing metadata dictionaries
        """
        submissions = self._get_company_submissions(company)
        
        # Get filings from submissions
        filings = submissions.get("filings", {}).get("recent", {})
        form_types = filings.get("form", [])
        filing_dates = filings.get("filingDate", [])
        accession_numbers = filings.get("accessionNumber", [])
        report_dates = filings.get("reportDate", [])
        
        # Filter for 10-Q filings and collect metadata
        # Also collect 10-K as fallback
        ten_q_filings = []
        ten_k_filings = []
        
        for i, form_type in enumerate(form_types):
            if form_type:
                form_upper = form_type.upper()
                if "10-Q" in form_upper:
                    if i < len(accession_numbers) and i < len(filing_dates):
                        ten_q_filings.append({
                            "form": form_type,
                            "filingDate": filing_dates[i],
                            "accessionNumber": accession_numbers[i],
                            "reportDate": report_dates[i] if i < len(report_dates) else filing_dates[i],
                        })
                elif "10-K" in form_upper:
                    if i < len(accession_numbers) and i < len(filing_dates):
                        ten_k_filings.append({
                            "form": form_type,
                            "filingDate": filing_dates[i],
                            "accessionNumber": accession_numbers[i],
                            "reportDate": report_dates[i] if i < len(report_dates) else filing_dates[i],
                        })
        
        # Sort by filing date (newest first)
        ten_q_filings.sort(key=lambda x: x["filingDate"], reverse=True)
        ten_k_filings.sort(key=lambda x: x["filingDate"], reverse=True)
        
        # Log findings
        total_filings = len(form_types)
        print(f"     Found {total_filings} total filings")
        print(f"     Found {len(ten_q_filings)} 10-Q filings")
        if ten_k_filings:
            print(f"     Found {len(ten_k_filings)} 10-K filings (fallback)")
        
        # Prefer 10-Q, but use 10-K if no 10-Q available
        if ten_q_filings:
            result = ten_q_filings[:limit]
            print(f"     Returning {len(result)} 10-Q filings: {[f['accessionNumber'] for f in result]}")
            return result
        elif ten_k_filings:
            # Return 10-K filings as fallback (note: these are annual, not quarterly)
            result = ten_k_filings[:limit]
            print(f"     Returning {len(result)} 10-K filings (fallback): {[f['accessionNumber'] for f in result]}")
            return result
        else:
            print(f"     ⚠️  No 10-Q or 10-K filings found")
            return []
    
    def get_available_filings(self, company: Company) -> tuple[list, list]:
        """
        Get list of available 10-Q and 10-K filings for a company.
        
        Returns:
            Tuple of (filings_10q, filings_10k) where each is a list of
            {"period": "2024-Q3", "date": "2024-10-31", "accession": "..."}
        """
        submissions = self._get_company_submissions(company)
        
        filings = submissions.get("filings", {}).get("recent", {})
        form_types = filings.get("form", [])
        filing_dates = filings.get("filingDate", [])
        accession_numbers = filings.get("accessionNumber", [])
        report_dates = filings.get("reportDate", [])
        
        filings_10q = []
        filings_10k = []
        
        for i, form_type in enumerate(form_types):
            if not form_type or i >= len(accession_numbers):
                continue
                
            form_upper = form_type.upper()
            report_date = report_dates[i] if i < len(report_dates) else filing_dates[i]
            filing_date = filing_dates[i] if i < len(filing_dates) else ""
            accession = accession_numbers[i]
            
            # Parse report date - use actual period end for clarity
            # (Companies have different fiscal years, so calendar quarters can be misleading)
            try:
                from datetime import datetime
                rd = datetime.strptime(report_date, "%Y-%m-%d")
                # Format as "Mar 2025" or "Nov 2024" for better readability
                period_label = rd.strftime("%b %Y")
            except:
                period_label = report_date
            
            if "10-Q" in form_upper:
                filings_10q.append({
                    "period": period_label,
                    "date": filing_date,
                    "accession": accession,
                    "report_date": report_date
                })
            elif "10-K" in form_upper and "/A" not in form_upper:  # Exclude amendments
                # For 10-K, include "FY" prefix
                filings_10k.append({
                    "period": f"FY {period_label}",
                    "date": filing_date,
                    "accession": accession,
                    "report_date": report_date
                })
        
        # Sort by report date (newest first) and limit to reasonable history
        filings_10q.sort(key=lambda x: x["report_date"], reverse=True)
        filings_10k.sort(key=lambda x: x["report_date"], reverse=True)
        
        # Limit to ~3 years of history
        return filings_10q[:12], filings_10k[:5]
    
    def fetch_filings_by_type(
        self,
        company: Company,
        filing_type: str = "10-Q",
        period: str = None
    ) -> tuple:
        """
        Fetch two consecutive filings of the specified type.
        
        Args:
            company: Company entity
            filing_type: "10-Q" or "10-K"
            period: Optional specific period (e.g., "2024-Q3" or "2024")
                   If None, uses the latest available
        
        Returns:
            Tuple of (current_filing, previous_filing)
        """
        filings_10q, filings_10k = self.get_available_filings(company)
        
        if filing_type == "10-K":
            available = filings_10k
        else:
            available = filings_10q
        
        if len(available) < 2:
            raise ValueError(
                f"Only found {len(available)} {filing_type} filing(s) for {company.ticker}. "
                f"Need at least 2 for comparison."
            )
        
        # Find the target filing
        if period:
            # Find specific period
            target_idx = None
            for i, f in enumerate(available):
                if f["period"] == period:
                    target_idx = i
                    break
            
            if target_idx is None:
                periods = [f["period"] for f in available]
                raise ValueError(
                    f"Period '{period}' not found. Available: {periods}"
                )
            
            if target_idx >= len(available) - 1:
                raise ValueError(
                    f"No previous filing available for comparison with {period}"
                )
            
            current_meta = available[target_idx]
            previous_meta = available[target_idx + 1]
        else:
            # Use latest two
            current_meta = available[0]
            previous_meta = available[1]
        
        print(f"  📋 Selected {filing_type} periods:")
        print(f"     Current: {current_meta['period']} (filed {current_meta['date']})")
        print(f"     Previous: {previous_meta['period']} (filed {previous_meta['date']})")
        
        # Download both filings
        filings = []
        for meta in [current_meta, previous_meta]:
            accession = meta["accession"]
            
            # Check cache first
            if self._cache.is_cached(company.ticker, accession):
                text_path = self._cache.get_cached_text_path(company.ticker, accession)
                if text_path and text_path.exists():
                    filing = Filing(
                        company=company,
                        accession=accession,
                        filing_date=meta["date"],
                        period_end=meta["report_date"],
                        filing_type=filing_type,
                        raw_text_path=text_path
                    )
                    filings.append(filing)
                    print(f"     ✓ Using cached filing {meta['period']}")
                    continue
            
            # Download
            try:
                print(f"     ⬇ Downloading {meta['period']}...")
                filing_dir = self._cache.get_filing_path(company.ticker, accession)
                filing_dir.mkdir(parents=True, exist_ok=True)
                
                content = self._download_filing_document(company, accession)
                text_path = filing_dir / "filing.txt"
                text_path.write_bytes(content)
                
                filing = Filing(
                    company=company,
                    accession=accession,
                    filing_date=meta["date"],
                    period_end=meta["report_date"],
                    filing_type=filing_type,
                    raw_text_path=text_path
                )
                filings.append(filing)
                print(f"     ✓ Downloaded {meta['period']}")
            except Exception as e:
                raise ValueError(f"Failed to download {meta['period']}: {e}")
        
        return tuple(filings)
    
    def _download_filing_document(self, company: Company, accession: str) -> bytes:
        """
        Download filing document with improved error handling and fallbacks.
        
        FIXED: Better fallback chain, more lenient validation, accepts XBRL-enhanced HTML
        
        Preconditions:
        - company has valid CIK
        - accession is a valid SEC accession number
        
        Postconditions:
        - Returns filing document as bytes
        - Raises requests.RequestException on download failure
        
        Args:
            company: Company entity
            accession: SEC accession number (e.g., "0000320193-23-000077")
            
        Returns:
            Filing document content as bytes
            
        Raises:
            requests.RequestException: On download failure
        """
        # Parse accession
        parts = accession.split("-")
        if len(parts) < 3:
            raise ValueError(f"Invalid accession format: {accession}")
        
        cik_clean = parts[0].lstrip("0") or "0"
        accession_clean = accession.replace("-", "")
        base_url = f"{self.ARCHIVES_BASE}/{cik_clean}/{accession_clean}"
        
        # Create session
        session = requests.Session()
        session.headers.update({
            "User-Agent": self._user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        })
        
        # Add retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        print(f"       Downloading {accession}...")
        
        # STRATEGY 1: Try complete submission text file first (most reliable)
        print(f"       Strategy 1: Complete submission .txt file")
        complete_text_url = f"{base_url}/{accession_clean}.txt"
        try:
            time.sleep(0.3)
            response = session.get(complete_text_url, timeout=30)
            if response.status_code == 200 and len(response.content) > 50000:
                is_valid, reason = self._is_valid_filing_content(response.content)
                if is_valid:
                    print(f"       ✅ Success with .txt file: {reason}")
                    return response.content
                else:
                    print(f"       ⚠️  .txt file rejected: {reason}")
        except Exception as e:
            print(f"       ⚠️  .txt file failed: {str(e)[:50]}")
        
        # STRATEGY 2: Try FilingSummary.xml approach - prioritize instance document
        print(f"       Strategy 2: FilingSummary.xml")
        summary_url = f"{base_url}/FilingSummary.xml"
        try:
            time.sleep(0.3)
            response = session.get(summary_url, timeout=30)
            
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.content, 'xml')
                reports = soup.find_all('Report')
                
                if reports:
                    print(f"       Found {len(reports)} reports in FilingSummary.xml")
                    
                    # PRIORITY 1: Get the instance document (the actual 10-Q filing)
                    # This is the BEST option - contains the full 10-Q without XBRL pop-ups
                    instance_file = None
                    for report in reports:
                        instance_attr = report.get('instance')
                        if instance_attr and instance_attr.endswith('.htm'):
                            instance_file = instance_attr
                            break
                    
                    if instance_file:
                        doc_url = f"{base_url}/{instance_file}"
                        print(f"       Trying instance document: {instance_file}")
                        try:
                            time.sleep(0.3)
                            doc_response = session.get(doc_url, timeout=30)
                            if doc_response.status_code == 200 and len(doc_response.content) > 20000:
                                is_valid, reason = self._is_valid_filing_content(doc_response.content)
                                if is_valid:
                                    print(f"       ✅ Success with instance document: {instance_file} - {reason}")
                                    return doc_response.content
                                else:
                                    print(f"       ⚠️  Instance document rejected: {reason}")
                        except Exception as e:
                            print(f"       ⚠️  Instance document failed: {str(e)[:50]}")
                    
                    # PRIORITY 2: Fall back to R*.htm files from FilingSummary
                    # These contain XBRL pop-ups but have the financial data
                    candidates = []
                    for report in reports:
                        html_name = report.find('HtmlFileName')
                        short_name = report.find('ShortName')
                        long_name = report.find('LongName')
                        
                        if html_name:
                            filename = html_name.get_text().strip()
                            short = short_name.get_text().strip() if short_name else ''
                            long = long_name.get_text().strip() if long_name else ''
                            
                            # Skip pure XML files
                            if filename.lower().endswith('.xml'):
                                continue
                            
                            # Prioritize
                            priority = 5
                            combined = (short + ' ' + long).lower()
                            
                            if 'complete' in combined or 'submission' in combined:
                                priority = 1
                            elif 'statement' in combined and 'operation' in combined:
                                priority = 2  # Income statement
                            elif '10-q' in combined or '10q' in combined:
                                priority = 3
                            elif 'document' in combined:
                                priority = 4
                            
                            candidates.append((priority, filename, short, long))
                    
                    # Try candidates
                    candidates.sort(key=lambda x: x[0])
                    for priority, filename, short, long in candidates[:5]:
                        doc_url = f"{base_url}/{filename}"
                        print(f"       Trying: {filename}")
                        
                        try:
                            time.sleep(0.3)
                            doc_response = session.get(doc_url, timeout=30)
                            
                            if doc_response.status_code == 200:
                                is_valid, reason = self._is_valid_filing_content(doc_response.content)
                                if is_valid:
                                    print(f"       ✅ Success: {filename} - {reason}")
                                    return doc_response.content
                                else:
                                    print(f"       ⚠️  Rejected: {reason}")
                        except Exception as e:
                            print(f"       ⚠️  Failed: {str(e)[:50]}")
                            continue
        except Exception as e:
            print(f"       ⚠️  FilingSummary.xml failed: {str(e)[:50]}")
        
        # STRATEGY 3: Try index.htm
        print(f"       Strategy 3: index.htm")
        index_urls = [
            f"{base_url}/{accession_clean}-index.htm",
            f"{base_url}/index.html"
        ]
        
        for index_url in index_urls:
            try:
                time.sleep(0.3)
                response = session.get(index_url, timeout=30)
                
                if response.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find document links
                    links = []
                    for row in soup.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            link = cells[0].find('a', href=True)
                            if link:
                                href = link.get('href', '')
                                desc = cells[1].get_text().strip() if len(cells) > 1 else ''
                                
                                if ('.htm' in href.lower() or '.html' in href.lower()):
                                    priority = self._calculate_document_priority(href, desc)
                                    links.append((priority, href, desc))
                    
                    links.sort(key=lambda x: x[0])
                    
                    for priority, href, desc in links[:5]:
                        # Build full URL
                        if href.startswith('http'):
                            doc_url = href
                        else:
                            doc_url = f"{base_url}/{href.lstrip('/')}"
                        
                        print(f"       Trying: {href}")
                        
                        try:
                            time.sleep(0.3)
                            doc_response = session.get(doc_url, timeout=30)
                            
                            if doc_response.status_code == 200:
                                is_valid, reason = self._is_valid_filing_content(doc_response.content)
                                if is_valid:
                                    print(f"       ✅ Success from index: {href} - {reason}")
                                    return doc_response.content
                                else:
                                    print(f"       ⚠️  Rejected: {reason}")
                        except Exception as e:
                            print(f"       ⚠️  Failed: {str(e)[:50]}")
                            continue
                    
                    break  # Tried this index, move on
            except:
                continue
        
        raise requests.RequestException(f"All download strategies failed for {accession}")
    
    def fetch_latest_two_10q(self, company: Company) -> tuple[Filing, Filing]:
        """
        Fetch the latest two 10-Q filings for a company.
        
        This is the main public method for getting filings. It handles
        caching, downloading, and creating Filing objects.
        
        Preconditions:
        - company has valid CIK and ticker
        
        Postconditions:
        - Returns tuple of (latest, previous) Filing objects
        - Both filings are cached locally
        - Raises ValueError if less than 2 filings found
        - Raises requests.RequestException on network errors
        
        Args:
            company: Company entity to fetch filings for
            
        Returns:
            Tuple of (latest Filing, previous Filing)
            
        Raises:
            ValueError: If less than 2 10-Q filings available
            requests.RequestException: On network or download errors
        """
        # Try up to 15 filings to find 2 that work
        filings_metadata = self._get_latest_10q_filings(company, limit=15)
        
        if len(filings_metadata) < 2:
            raise ValueError(
                f"Only found {len(filings_metadata)} 10-Q filing(s) for {company.ticker}. "
                f"Need at least 2 for comparison."
            )
        
        successful_filings = []
        print(f"     Attempting to download filings (need 2 successful)...")
        
        for i, meta in enumerate(filings_metadata):
            if len(successful_filings) >= 2:
                break
            
            accession = meta["accessionNumber"]
            print(f"     [{i+1}/{len(filings_metadata)}] Trying {accession}...")
            
            # Check cache first
            if self._cache.is_cached(company.ticker, accession):
                text_path = self._cache.get_cached_text_path(company.ticker, accession)
                if text_path and text_path.exists():
                    filing = Filing(
                        company=company,
                        accession=accession,
                        filing_date=self._parse_filing_date(meta["filingDate"]),
                        period_end=self._parse_filing_date(meta["reportDate"]),
                        filing_type="10-Q",
                        raw_text_path=text_path
                    )
                    successful_filings.append(filing)
                    print(f"     ✅ Using cached filing {accession}")
                    continue
            
            # Try to download
            try:
                filing_dir = self._cache.get_filing_path(company.ticker, accession)
                filing_dir.mkdir(parents=True, exist_ok=True)
                
                content = self._download_filing_document(company, accession)
                
                # Validate using the new method
                is_valid, reason = self._is_valid_filing_content(content)
                if not is_valid:
                    print(f"     ❌ Validation failed: {reason}")
                    raise requests.RequestException(f"Validation failed: {reason}")
                
                # Save to cache
                text_path = filing_dir / "filing.txt"
                text_path.write_bytes(content)
                
                filing = Filing(
                    company=company,
                    accession=accession,
                    filing_date=self._parse_filing_date(meta["filingDate"]),
                    period_end=self._parse_filing_date(meta["reportDate"]),
                    filing_type="10-Q",
                    raw_text_path=text_path
                )
                successful_filings.append(filing)
                print(f"     ✅ Successfully downloaded {accession} ({len(content)} bytes)")
                
            except Exception as e:
                error_msg = str(e)
                print(f"     ❌ Failed {accession}: {error_msg[:100]}")
                continue
        
        if len(successful_filings) < 2:
            raise ValueError(
                f"Could not download 2 available 10-Q filings for {company.ticker}. "
                f"Successfully downloaded {len(successful_filings)} filing(s). "
                f"Tried {len(filings_metadata)} different filings. "
                f"This may indicate SEC website issues or overly strict validation. "
                f"Try: (1) Wait and retry, (2) Check SEC website manually, "
                f"(3) Try a different company like AAPL or NVDA"
            )
        
        return tuple(successful_filings[:2])
