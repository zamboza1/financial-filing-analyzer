"""Text extraction and cleaning from SEC filings."""

import re
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup


class TextExtractor:
    """
    Extracts clean text from SEC filing documents.
    
    SEC filings come in various formats (HTML, plain text, XBRL).
    This class handles extraction and basic cleaning to produce
    "good-enough" text for chunking and analysis.
    
    Representation Invariants:
    - Extracted text is non-empty after processing
    """
    
    def __init__(self) -> None:
        """Initialize text extractor."""
        pass
    
    def extract_from_file(self, file_path: Path) -> str:
        """
        Extract clean text from a filing file.
        
        Preconditions:
        - file_path exists and is readable
        
        Postconditions:
        - Returns non-empty cleaned text
        - Raises FileNotFoundError if file doesn't exist
        - Raises ValueError if extraction fails
        
        Args:
            file_path: Path to filing document
            
        Returns:
            Cleaned text content
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If extraction fails
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Filing file not found: {file_path}")
        
        content = file_path.read_bytes()
        
        # Try to detect if it's HTML
        try:
            text = content.decode('utf-8', errors='ignore')
            
            # Check if this is a complete submission text file (contains <DOCUMENT> tags)
            if '<DOCUMENT>' in text and '</DOCUMENT>' in text:
                # This is a complete submission file with multiple documents
                # Extract the main 10-Q document, not XBRL
                return self._extract_from_submission_file(text)
            
            if self._is_html(text):
                return self._extract_from_html(text)
            else:
                return self._clean_plain_text(text)
        except Exception as e:
            raise ValueError(f"Failed to extract text from {file_path}: {e}") from e
    
    def _extract_from_submission_file(self, submission_text: str) -> str:
        """
        Extract the main 10-Q document from a complete submission file.
        
        Complete submission files contain multiple <DOCUMENT> sections.
        We want the main 10-Q HTML document, not XBRL instance documents.
        
        Args:
            submission_text: Complete submission file content
            
        Returns:
            Text from the main 10-Q document
        """
        import re
        
        # Split into document sections
        # Documents are separated by <DOCUMENT>...</DOCUMENT> tags
        document_pattern = r'<DOCUMENT>(.*?)</DOCUMENT>'
        documents = re.findall(document_pattern, submission_text, re.DOTALL | re.IGNORECASE)
        
        if not documents:
            # No document tags found, return as-is
            return self._clean_plain_text(submission_text)
        
        # Find the main 10-Q document (not XBRL)
        main_document = None
        main_document_priority = 999
        
        for doc in documents:
            doc_lower = doc.lower()
            
            # Skip XBRL documents
            if 'xbrl' in doc_lower or 'xml' in doc_lower[:200]:
                # Check if it's explicitly marked as XBRL
                if 'idea: xbrl document' in doc_lower or 'type>xml' in doc_lower:
                    continue
            
            # Look for document type and description
            doc_type_match = re.search(r'<TYPE>(.*?)</TYPE>', doc, re.IGNORECASE)
            desc_match = re.search(r'<DESCRIPTION>(.*?)</DESCRIPTION>', doc, re.IGNORECASE)
            
            doc_type = doc_type_match.group(1).strip().lower() if doc_type_match else ''
            description = desc_match.group(1).strip().lower() if desc_match else ''
            
            # Priority: 10-Q HTML > 10-Q > HTML > other
            priority = 999
            if '10-q' in description or '10-q' in doc_type:
                if 'html' in doc_type or 'htm' in description or '<html' in doc[:500].lower():
                    priority = 1  # Best: 10-Q HTML
                else:
                    priority = 2  # Good: 10-Q other format
            elif 'html' in doc_type or 'htm' in description or '<html' in doc[:500].lower():
                priority = 3  # OK: HTML but not explicitly 10-Q
            elif '10-k' in description or '10-k' in doc_type:
                priority = 4  # Fallback: 10-K
            
            if priority < main_document_priority:
                main_document = doc
                main_document_priority = priority
        
        # If we found a main document, extract its TEXT section
        if main_document:
            text_match = re.search(r'<TEXT>(.*?)</TEXT>', main_document, re.DOTALL | re.IGNORECASE)
            if text_match:
                document_text = text_match.group(1)
                # Now process this as HTML or plain text
                if self._is_html(document_text):
                    return self._extract_from_html(document_text)
                else:
                    return self._clean_plain_text(document_text)
            else:
                # No TEXT tag, use the whole document
                if self._is_html(main_document):
                    return self._extract_from_html(main_document)
                else:
                    return self._clean_plain_text(main_document)
        
        # Fallback: if no good document found, try the first non-XBRL one
        for doc in documents:
            if 'xbrl' not in doc.lower()[:500] and 'xml' not in doc.lower()[:200]:
                text_match = re.search(r'<TEXT>(.*?)</TEXT>', doc, re.DOTALL | re.IGNORECASE)
                if text_match:
                    document_text = text_match.group(1)
                    if self._is_html(document_text):
                        return self._extract_from_html(document_text)
                    else:
                        return self._clean_plain_text(document_text)
        
        # Last resort: return first document's text
        if documents:
            text_match = re.search(r'<TEXT>(.*?)</TEXT>', documents[0], re.DOTALL | re.IGNORECASE)
            if text_match:
                return self._clean_plain_text(text_match.group(1))
        
        # If all else fails, return the whole submission file
        return self._clean_plain_text(submission_text)
    
    def _is_html(self, text: str) -> bool:
        """
        Check if text appears to be HTML.
        
        FIXED: Check more than first 1KB to avoid mis-detection
        
        Args:
            text: Text content to check
            
        Returns:
            True if appears to be HTML, False otherwise
        """
        # Simple heuristic: look for HTML tags
        html_pattern = re.compile(r'<[a-z][\s\S]*>', re.IGNORECASE)
        return bool(html_pattern.search(text[:20000]))  # Check first 20KB
    
    def _extract_from_html(self, html_content: str) -> str:
        """
        Extract text from HTML content.
        
        Uses BeautifulSoup to parse HTML and extract text,
        removing scripts, styles, navigation, and other non-content elements.
        Also handles XBRL/XML content.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Cleaned text extracted from HTML
        """
        # Check if this is XBRL HTML (has XBRL indicators but is HTML)
        html_lower = html_content.lower()
        is_xbrl_html = (
            'entity information [line items]' in html_lower or
            'xbrl document' in html_lower or
            'period type:' in html_lower or
            (html_content.strip().startswith('<?xml') or '<xbrl' in html_lower)
        )
        
        if is_xbrl_html:
            # This is XBRL HTML - extract readable text from it
            return self._extract_from_xbrl(html_content)
        
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Remove script and style elements
        for script in soup(["script", "style", "meta", "link", "noscript"]):
            script.decompose()
        
        # Remove XBRL reference/definition sections (hidden divs with pop-up content)
        # These contain text like "+ References", "- Definition", "defref_us-gaap_"
        for element in soup.find_all('div', style=re.compile(r'display:\s*none', re.I)):
            element.decompose()
        
        # Remove elements with class="authRefData" (XBRL reference data)
        for element in soup.find_all(class_='authRefData'):
            element.decompose()
        
        # Remove tables that are XBRL reference data (hidden tables with id like "defref_...")
        for element in soup.find_all('table', id=re.compile(r'^defref_', re.I)):
            element.decompose()
        
        # Remove navigation elements (common SEC website patterns)
        nav_keywords = [
            'navigation', 'nav', 'menu', 'header', 'footer', 'sidebar',
            'skip to main content', 'search options', 'quick edgar tutorial',
            'company filings search', 'site map', 'accessibility'
        ]
        
        # Remove elements with navigation-related classes/ids
        for element in soup.find_all(['nav', 'header', 'footer']):
            element.decompose()
        
        # Remove elements with common SEC navigation classes
        for element in soup.find_all(class_=re.compile(r'nav|menu|header|footer|sidebar', re.I)):
            element.decompose()
        
        # Remove links that are clearly navigation
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()
            # Remove navigation links
            if any(keyword in text for keyword in ['site map', 'accessibility', 'privacy', 'contact', 'careers']):
                link.decompose()
            # Remove links to SEC website sections
            if 'sec.gov' in href and ('/about' in href or '/divisions' in href or '/careers' in href):
                link.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Remove SEC website navigation text patterns
        sec_nav_patterns = [
            r'Directory List of.*?Search Options',
            r'Skip to Main Content.*?About What We Do',
            r'Quick EDGAR Tutorial.*?Company Filings',
            r'Site Map.*?Accessibility.*?Contracts.*?Privacy',
            r'Investor\.gov.*?USA\.gov',
            r'No FEAR Act.*?EEO Data',
            r'Open Government.*?Plain Writing',
        ]
        
        for pattern in sec_nav_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove common SEC website text
        sec_text_to_remove = [
            'Directory List of',
            'Search Options',
            'Skip to Main Content',
            'About What We Do',
            'Commissioners',
            'Securities Laws',
            'Reports',
            'Careers',
            'Contact',
            'Divisions',
            'Corporation Finance',
            'Enforcement',
            'Investment Management',
            'Trading and Markets',
            'Quick EDGAR Tutorial',
            'Company Filings Search',
            'Requesting Public Documents',
            'Site Map',
            'Accessibility',
            'Contracts',
            'Privacy',
            'Inspector General',
            'FOIA',
            'No FEAR Act',
            'EEO Data',
            'Open Government',
            'Plain Writing',
            'Investor.gov',
            'USA.gov',
        ]
        
        for remove_text in sec_text_to_remove:
            text = text.replace(remove_text, '')
        
        # Clean up whitespace
        return self._clean_plain_text(text)
    
    def _extract_from_xbrl(self, xml_content: str) -> str:
        """
        Extract readable text from XBRL/XML content.
        
        CRITICAL FIX: Properly extracts financial data from XBRL HTML tables.
        Formats data as "Label: $value1, $value2" for KPI pattern matching.
        Filters out XBRL definition/reference pop-ups.
        
        Args:
            xml_content: XBRL/XML content as string
            
        Returns:
            Text extracted from XBRL in readable format
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(xml_content, 'html.parser')
            
            # FIRST: Remove XBRL definition/reference sections
            # These are hidden divs that contain "+ References", "- Definition" pop-ups
            for element in soup.find_all('div', style=re.compile(r'display:\s*none', re.I)):
                element.decompose()
            
            # Remove tables with defref_ IDs (XBRL reference data)
            for element in soup.find_all('table', id=re.compile(r'^defref_', re.I)):
                element.decompose()
            
            # Remove elements with class="authRefData"
            for element in soup.find_all(class_='authRefData'):
                element.decompose()
            
            # Remove scripts
            for element in soup.find_all(['script', 'style']):
                element.decompose()
            
            # XBRL HTML has tables with financial data
            financial_keywords = [
                'revenue', 'sales', 'net income', 'operating income', 'earnings',
                'income statement', 'balance sheet', 'cash flow', 'financial',
                'consolidated', 'condensed', 'statement of operations'
            ]
            
            text_parts = []
            
            # Find all tables
            tables = soup.find_all('table')
            for table in tables:
                # Get table text to check if it contains financial data
                table_text = table.get_text().lower()
                
                # Check if this table has financial content
                has_financial = any(keyword in table_text for keyword in financial_keywords)
                
                if has_financial or 'report' in str(table.get('class', [])).lower():
                    # Extract data from this table row by row
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            # Extract label from first cell (remove HTML tags)
                            label_cell = cells[0]
                            label = label_cell.get_text(separator=' ', strip=True)
                            # Clean up label
                            label = re.sub(r'\s+', ' ', label).strip()
                            
                            # Get all value cells (there may be multiple periods)
                            values = []
                            for cell in cells[1:]:
                                # Get text and clean it
                                value = cell.get_text(separator='', strip=True)
                                # Remove HTML entities and clean
                                value = value.replace('&nbsp;', '').replace('&#160;', '')
                                value = value.replace('(', '-').replace(')', '')  # Handle negative numbers
                                value = re.sub(r'\s+', '', value)  # Remove all whitespace
                                
                                # Keep the value if it looks like a number
                                if value and value not in ['—', '-', '']:
                                    # Check if it starts with $ or is a number
                                    if value.startswith('$') or re.match(r'^[\d,.-]+', value):
                                        values.append(value)
                            
                            # Format as "Label: $value1, $value2" for pattern matching
                            if label and values:
                                # Join values with commas
                                value_str = ', '.join(values)
                                # Format: "Net sales: $94,036, $85,777, $313,695, $296,105"
                                text_parts.append(f"{label}: {value_str}")
            
            # If we found financial tables, use that
            if text_parts:
                combined = '\n'.join(text_parts)
                # Also extract all text for additional context
                all_text = soup.get_text(separator=' ', strip=True)
                # Combine structured data with full text
                combined = combined + '\n\n' + all_text
                return self._clean_plain_text(combined)
            
            # Fallback: Extract all table data as structured text
            all_table_data = []
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    row_data = []
                    for cell in cells:
                        text = cell.get_text(separator=' ', strip=True)
                        text = text.replace('&nbsp;', ' ').replace('&#160;', ' ')
                        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
                        if text:
                            row_data.append(text)
                    if row_data:
                        # Format as "Label | Value1 | Value2"
                        all_table_data.append(' | '.join(row_data))
            
            if all_table_data:
                return self._clean_plain_text('\n'.join(all_table_data))
            
            # Last resort: extract all text (but clean it)
            all_text = soup.get_text(separator=' ', strip=True)
            return self._clean_plain_text(all_text)
            
        except Exception as e:
            # If XBRL parsing fails, try to extract text anyway
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(xml_content, 'html.parser')
                all_text = soup.get_text(separator=' ', strip=True)
                return self._clean_plain_text(all_text)
            except:
                # Last resort: just return cleaned XML text
                return self._clean_plain_text(xml_content)
    
    def _clean_plain_text(self, text: str) -> str:
        """
        Clean plain text content.
        
        Removes excessive whitespace, normalizes line breaks,
        and removes common SEC filing artifacts and navigation text.
        
        Args:
            text: Raw text content
            
        Returns:
            Cleaned text
        """
        # Remove excessive whitespace (but preserve paragraph structure)
        # Replace multiple spaces with single space
        text = re.sub(r' +', ' ', text)
        
        # Normalize line breaks (keep single newlines, collapse multiple)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        
        # Remove common SEC artifacts
        # Remove page numbers (e.g., "Page 1 of 100")
        text = re.sub(r'Page \d+ of \d+', '', text, flags=re.IGNORECASE)
        
        # Remove form headers (e.g., "UNITED STATES SECURITIES AND EXCHANGE COMMISSION")
        # But keep the actual content
        
        # Remove excessive dashes/separators
        text = re.sub(r'-{3,}', '---', text)
        
        # Remove lines that are clearly navigation (short lines with common nav words)
        lines = text.split('\n')
        filtered_lines = []
        nav_keywords = ['site map', 'accessibility', 'privacy', 'contact', 'careers', 
                       'investor.gov', 'usa.gov', 'skip to', 'search options', 'directory list']
        
        for line in lines:
            line_lower = line.lower().strip()
            # Skip very short lines that are navigation
            if len(line_lower) < 50 and any(keyword in line_lower for keyword in nav_keywords):
                continue
            # Skip lines that are just navigation links
            if line_lower in ['about', 'what we do', 'commissioners', 'divisions', 'reports']:
                continue
            filtered_lines.append(line)
        
        text = '\n'.join(filtered_lines)
        
        # Check if this looks like an index page (has lots of navigation text)
        # If so, it's probably not the actual filing content
        nav_indicators = ['directory list', 'search options', 'skip to main content', 
                         'quick edgar tutorial', 'site map', 'accessibility']
        nav_count = sum(1 for indicator in nav_indicators if indicator in text.lower())
        
        if nav_count >= 3:
            # This looks like an index page, try to extract actual content
            # Look for sections that might contain filing content
            # SEC filings often have sections like "Item 1", "Item 2", etc.
            if 'item 1' not in text.lower() and 'item 2' not in text.lower():
                # Probably just navigation, warn but continue
                print(f"     ⚠️  Warning: Extracted text appears to be an index page, not filing content")
        
        # Trim whitespace from start/end
        text = text.strip()
        
        if not text:
            raise ValueError("Extracted text is empty after cleaning")
        
        return text
    
    def extract_from_filing(self, filing_path: Optional[Path]) -> str:
        """
        Extract text from a Filing's raw_text_path.
        
        Convenience method that handles Optional path.
        
        Preconditions:
        - filing_path is not None and points to valid file
        
        Postconditions:
        - Returns cleaned text
        - Raises ValueError if path is None or invalid
        
        Args:
            filing_path: Optional path to filing text file
            
        Returns:
            Cleaned text content
            
        Raises:
            ValueError: If path is None or file is invalid
        """
        if filing_path is None:
            raise ValueError("Filing path is None")
        
        return self.extract_from_file(filing_path)
