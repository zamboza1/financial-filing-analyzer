"""Caching layer for downloaded filings."""

from pathlib import Path
from typing import Optional


class FilingCache:
    """
    Manages caching of downloaded SEC filings.
    
    This class encapsulates the caching logic to avoid re-downloading
    filings that already exist locally.
    
    Representation Invariants:
    - cache_root is an absolute Path
    - All cached files are stored under cache_root/{ticker}/{accession}/
    """
    
    def __init__(self, cache_root: Path) -> None:
        """
        Initialize cache with root directory.
        
        Preconditions:
        - cache_root is a valid Path (will be created if doesn't exist)
        
        Postconditions:
        - cache_root directory exists (created if needed)
        - _cache_root is set to absolute path
        """
        self._cache_root = cache_root.resolve()
        self._cache_root.mkdir(parents=True, exist_ok=True)
    
    def get_filing_path(self, ticker: str, accession: str) -> Path:
        """
        Get the expected cache path for a filing.
        
        Args:
            ticker: Company ticker symbol
            accession: SEC accession number
            
        Returns:
            Path where filing should be cached
        """
        ticker_upper = ticker.upper()
        # Clean accession (remove dashes for directory name)
        accession_clean = accession.replace("-", "")
        return self._cache_root / ticker_upper / accession_clean
    
    def is_cached(self, ticker: str, accession: str) -> bool:
        """
        Check if a filing is already cached.
        
        Preconditions:
        - ticker and accession are non-empty strings
        
        Postconditions:
        - Returns True if filing directory exists and contains files
        - Returns False otherwise
        
        Args:
            ticker: Company ticker symbol
            accession: SEC accession number
            
        Returns:
            True if filing is cached, False otherwise
        """
        filing_dir = self.get_filing_path(ticker, accession)
        if not filing_dir.exists() or not filing_dir.is_dir():
            return False
        
        # Check if directory has any files (not just empty directory)
        return any(filing_dir.iterdir())
    
    def get_cached_text_path(self, ticker: str, accession: str) -> Optional[Path]:
        """
        Get path to cached text file if it exists.
        
        Looks for common text file names: filing.txt, document.txt, etc.
        
        Args:
            ticker: Company ticker symbol
            accession: SEC accession number
            
        Returns:
            Path to text file if found, None otherwise
        """
        filing_dir = self.get_filing_path(ticker, accession)
        if not filing_dir.exists():
            return None
        
        # Common text file names in SEC filings
        text_names = ["filing.txt", "document.txt", "complete.txt"]
        for name in text_names:
            text_path = filing_dir / name
            if text_path.exists() and text_path.is_file():
                return text_path
        
        # Fallback: look for any .txt file
        txt_files = list(filing_dir.glob("*.txt"))
        if txt_files:
            return txt_files[0]
        
        return None
    
    def mark_cached(self, ticker: str, accession: str, text_path: Path) -> None:
        """
        Mark a filing as cached by ensuring directory structure exists.
        
        This doesn't copy files, just ensures the directory is ready.
        The actual file saving is done by the caller.
        
        Args:
            ticker: Company ticker symbol
            accession: SEC accession number
            text_path: Path to the text file that was saved
        """
        filing_dir = self.get_filing_path(ticker, accession)
        filing_dir.mkdir(parents=True, exist_ok=True)
        # Directory is ready; file should already be at text_path
