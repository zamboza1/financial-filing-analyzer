"""Structured KPI extraction from SEC 10-Q filings."""

import re
from typing import List, Optional, Dict, Tuple
from backend.entities import DocumentChunk, KpiSnapshot, Filing


class KPIExtractor:
    """
    Extracts structured KPIs from SEC 10-Q document chunks.
    
    IMPORTANT: SEC filings typically report values "in millions" unless otherwise noted.
    This extractor normalizes all monetary values to millions USD.
    
    Key metrics extracted:
    - Revenue (Net Sales)
    - Operating Income
    - Net Income
    - EPS (Diluted)
    """
    
    def __init__(self) -> None:
        """Initialize KPI extractor."""
        pass
    
    def extract_from_chunks(
        self,
        chunks: List[DocumentChunk],
        period_end: str
    ) -> KpiSnapshot:
        """
        Extract KPIs from document chunks.
        
        Args:
            chunks: List of DocumentChunk objects
            period_end: Period end date (YYYY-MM-DD)
            
        Returns:
            KpiSnapshot with extracted KPIs
        """
        # Combine all chunk text for pattern matching
        all_text = "\n".join(chunk.text for chunk in chunks)
        
        # Detect if values are reported in millions or thousands
        unit_scale = self._detect_unit_scale(all_text)
        print(f"     📊 Detected unit scale: values in {unit_scale}")
        
        extracted: Dict[str, Tuple[float, str]] = {}
        source_chunk_ids: Dict[str, str] = {}
        
        # Extract each KPI from chunks
        for chunk in chunks:
            text = chunk.text
            
            # Core income statement metrics
            if 'revenue' not in extracted:
                value = self._extract_revenue(text, unit_scale)
                if value is not None:
                    extracted['revenue'] = (value, chunk.chunk_id)
                    source_chunk_ids['revenue'] = chunk.chunk_id
                    print(f"     ✓ Revenue: ${value:,.0f}M (from chunk {chunk.chunk_index})")
            
            if 'cost_of_revenue' not in extracted:
                value = self._extract_cost_of_revenue(text, unit_scale)
                if value is not None:
                    extracted['cost_of_revenue'] = (value, chunk.chunk_id)
                    source_chunk_ids['cost_of_revenue'] = chunk.chunk_id
                    print(f"     ✓ Cost of Revenue: ${value:,.0f}M")
            
            if 'gross_profit' not in extracted:
                value = self._extract_gross_profit(text, unit_scale)
                if value is not None:
                    extracted['gross_profit'] = (value, chunk.chunk_id)
                    source_chunk_ids['gross_profit'] = chunk.chunk_id
                    print(f"     ✓ Gross Profit: ${value:,.0f}M")
            
            if 'operating_income' not in extracted:
                value = self._extract_operating_income(text, unit_scale)
                if value is not None:
                    extracted['operating_income'] = (value, chunk.chunk_id)
                    source_chunk_ids['operating_income'] = chunk.chunk_id
                    print(f"     ✓ Operating Income: ${value:,.0f}M")
            
            if 'net_income' not in extracted:
                value = self._extract_net_income(text, unit_scale)
                if value is not None:
                    extracted['net_income'] = (value, chunk.chunk_id)
                    source_chunk_ids['net_income'] = chunk.chunk_id
                    print(f"     ✓ Net Income: ${value:,.0f}M")
            
            if 'eps' not in extracted:
                value = self._extract_eps(text)
                if value is not None:
                    extracted['eps'] = (value, chunk.chunk_id)
                    source_chunk_ids['eps'] = chunk.chunk_id
                    print(f"     ✓ EPS: ${value:.2f}")
            
            # Expense metrics
            if 'research_and_development' not in extracted:
                value = self._extract_rd_expense(text, unit_scale)
                if value is not None:
                    extracted['research_and_development'] = (value, chunk.chunk_id)
                    source_chunk_ids['research_and_development'] = chunk.chunk_id
                    print(f"     ✓ R&D Expense: ${value:,.0f}M")
            
            if 'selling_general_admin' not in extracted:
                value = self._extract_sga_expense(text, unit_scale)
                if value is not None:
                    extracted['selling_general_admin'] = (value, chunk.chunk_id)
                    source_chunk_ids['selling_general_admin'] = chunk.chunk_id
                    print(f"     ✓ SG&A Expense: ${value:,.0f}M")
            
            if 'depreciation_amortization' not in extracted:
                value = self._extract_depreciation(text, unit_scale)
                if value is not None:
                    extracted['depreciation_amortization'] = (value, chunk.chunk_id)
                    source_chunk_ids['depreciation_amortization'] = chunk.chunk_id
                    print(f"     ✓ D&A: ${value:,.0f}M")
            
            # Cash flow metrics
            if 'operating_cash_flow' not in extracted:
                value = self._extract_operating_cash_flow(text, unit_scale)
                if value is not None:
                    extracted['operating_cash_flow'] = (value, chunk.chunk_id)
                    source_chunk_ids['operating_cash_flow'] = chunk.chunk_id
                    print(f"     ✓ Operating Cash Flow: ${value:,.0f}M")
        
        # Apply sanity checks and corrections
        extracted = self._apply_sanity_checks(extracted)
        
        # Extract guidance
        guidance = self._extract_guidance(chunks)
        if guidance:
            for chunk in chunks:
                if 'guidance' in chunk.text.lower() or 'outlook' in chunk.text.lower():
                    source_chunk_ids['guidance'] = chunk.chunk_id
                    break
        
        # If we couldn't find KPIs, create minimal valid snapshot
        if not extracted and not guidance:
            guidance = "Financial metrics could not be extracted automatically. Please verify manually."
            if chunks:
                source_chunk_ids['guidance'] = chunks[0].chunk_id
        
        # Build snapshot
        def get_value(kpi_name: str) -> Optional[float]:
            result = extracted.get(kpi_name)
            return result[0] if isinstance(result, tuple) else result
        
        snapshot = KpiSnapshot(
            period_end=period_end,
            # Core income statement
            revenue=get_value('revenue'),
            cost_of_revenue=get_value('cost_of_revenue'),
            gross_profit=get_value('gross_profit'),
            operating_income=get_value('operating_income'),
            net_income=get_value('net_income'),
            eps=get_value('eps'),
            # Expenses
            research_and_development=get_value('research_and_development'),
            selling_general_admin=get_value('selling_general_admin'),
            depreciation_amortization=get_value('depreciation_amortization'),
            # Cash flow
            operating_cash_flow=get_value('operating_cash_flow'),
            # Qualitative
            guidance=guidance,
            segments=[],
            source_chunk_ids=source_chunk_ids
        )
        
        return snapshot
    
    def _detect_unit_scale(self, text: str) -> str:
        """
        Detect whether the filing reports values in millions, thousands, or actual dollars.
        
        SEC 10-Q filings almost always have a note like:
        - "(in millions, except per share amounts)"
        - "(in thousands)"
        - "(Dollars in millions)"
        
        Returns: 'millions', 'thousands', or 'dollars'
        """
        text_lower = text.lower()
        
        # Check for explicit declarations
        if re.search(r'\(\s*in\s+millions?\s*[,)]', text_lower):
            return 'millions'
        if re.search(r'\(\s*dollars?\s+in\s+millions?\s*[,)]', text_lower):
            return 'millions'
        if re.search(r'amounts?\s+in\s+millions?', text_lower):
            return 'millions'
        if re.search(r'\(\s*in\s+thousands?\s*[,)]', text_lower):
            return 'thousands'
        if re.search(r'\(\s*dollars?\s+in\s+thousands?\s*[,)]', text_lower):
            return 'thousands'
        
        # Default for SEC filings is millions
        return 'millions'
    
    def _normalize_to_millions(self, value: float, unit_scale: str) -> float:
        """Convert value to millions based on detected unit scale."""
        if unit_scale == 'thousands':
            return value / 1000
        elif unit_scale == 'dollars':
            return value / 1000000
        # Already in millions
        return value
    
    def _extract_revenue(self, text: str, unit_scale: str) -> Optional[float]:
        """
        Extract revenue/net sales from text.
        
        SEC 10-Q format example:
        "Total net sales | 95,359 |  |  | 90,753"
        """
        patterns = [
            # Generic: Find first number after "Total net sales"
            r'total\s+net\s+sales[^0-9]*(\d[\d,]+)',
            # "Net sales" followed by number (but not "cost of net sales")
            r'(?<!cost of )net\s+sales[^0-9]*(\d[\d,]+)',
            # "Total revenue" followed by number
            r'total\s+(?:net\s+)?revenue[s]?[^0-9]*(\d[\d,]+)',
            # "Revenue:" followed by number
            r'\brevenue[s]?\s*[:][^0-9]*(\d[\d,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    
                    # Normalize to millions
                    value = self._normalize_to_millions(value, unit_scale)
                    
                    # Sanity check: revenue should be positive and reasonable
                    # For Fortune 500, typically $1B to $500B per quarter
                    if 1000 <= value <= 500000:  # $1B to $500B
                        return value
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_net_income(self, text: str, unit_scale: str) -> Optional[float]:
        """
        Extract net income from text.
        
        SEC 10-Q format example:
        "Net income | $ | 24,780 |  |"
        """
        patterns = [
            # Generic: Find first number after "Net income"
            # Handles: "Net income | $ | 24,780" and "Net income: 24,780"
            r'\bnet\s+income[^0-9]*(\d[\d,]+)',
            # "Net earnings" followed by number
            r'\bnet\s+earnings[^0-9]*(\d[\d,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    
                    # Skip very small values - these are likely something else
                    if value < 100:  # Less than $100M for a large company is suspicious
                        continue
                    
                    # Normalize to millions
                    value = self._normalize_to_millions(value, unit_scale)
                    
                    # Sanity check: net income should be reasonable
                    # Can be negative (loss) but typically -$50B to +$50B per quarter
                    if -50000 <= value <= 50000:  # -$50B to $50B
                        return value
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_operating_income(self, text: str, unit_scale: str) -> Optional[float]:
        """
        Extract operating income from text.
        
        SEC 10-Q format example:
        "Operating income | 29,589 |  |  | 27,900"
        """
        patterns = [
            # Generic: Find first number after "Operating income"
            r'\boperating\s+income[^0-9]*(\d[\d,]+)',
            # "Income from operations" followed by number
            r'\bincome\s+from\s+operations[^0-9]*(\d[\d,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    
                    # Skip very small values
                    if value < 100:
                        continue
                    
                    # Normalize to millions
                    value = self._normalize_to_millions(value, unit_scale)
                    
                    # Sanity check
                    if -100000 <= value <= 100000:  # -$100B to $100B
                        return value
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_eps(self, text: str) -> Optional[float]:
        """
        Extract earnings per share from text.
        
        EPS is reported in actual dollars, not millions.
        
        SEC 10-Q format example:
        "Diluted | $ | 1.65 | $ | 1.53"
        """
        patterns = [
            # Generic: Find first decimal number after "Diluted" in EPS section
            # This handles: "Diluted | $ | 1.65"
            r'\bdiluted[^0-9]*(\d+\.\d+)',
            # "Earnings per share" sections
            r'earnings\s+per\s+share[^0-9]*diluted[^0-9]*(\d+\.\d+)',
            # "Basic and diluted" followed by number
            r'basic\s+and\s+diluted[^0-9]*(\d+\.\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    value = float(match.group(1))
                    
                    # Sanity check: EPS typically $0.01 to $50
                    if 0.01 <= abs(value) <= 50:
                        return value
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _apply_sanity_checks(self, extracted: Dict[str, Tuple[float, str]]) -> Dict[str, Tuple[float, str]]:
        """
        Apply sanity checks to extracted values and fix obvious errors.
        
        Rules:
        - Net income should typically be 5-40% of revenue
        - Operating income should be between net income and revenue
        - If net income > revenue, something is wrong
        """
        revenue = extracted.get('revenue', (None,))[0]
        net_income = extracted.get('net_income', (None,))[0]
        operating_income = extracted.get('operating_income', (None,))[0]
        
        if revenue and net_income:
            margin = net_income / revenue
            
            # Net income should typically be < 50% of revenue
            # If margin > 100%, net income is likely wrong
            if margin > 1.0:
                print(f"     ⚠️ Sanity check failed: net income (${net_income:,.0f}M) > revenue (${revenue:,.0f}M)")
                print(f"     ⚠️ Removing suspicious net income value")
                del extracted['net_income']
            
            # If margin is suspiciously low (< 0.5%), might be wrong
            elif margin < 0.005 and revenue > 10000:  # Large company
                print(f"     ⚠️ Sanity check: very low margin ({margin:.2%}). Net income might be incorrect.")
        
        if revenue and operating_income:
            op_margin = operating_income / revenue
            if op_margin > 0.7:  # 70% operating margin is extremely rare
                print(f"     ⚠️ Sanity check: unusually high operating margin ({op_margin:.1%})")
        
        return extracted
    
    def _extract_guidance(self, chunks: List[DocumentChunk]) -> Optional[str]:
        """Extract guidance/outlook text."""
        guidance_keywords = ['guidance', 'outlook', 'expect', 'forecast']
        
        for chunk in chunks:
            text = chunk.text
            sentences = re.split(r'[.!?]+', text)
            
            for sentence in sentences:
                sentence_lower = sentence.lower()
                if any(keyword in sentence_lower for keyword in guidance_keywords):
                    guidance = sentence.strip()
                    if 20 < len(guidance) < 500:
                        return guidance
        
        return None
    
    # ========== Additional Metric Extraction Methods ==========
    
    def _extract_cost_of_revenue(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract cost of sales/revenue."""
        patterns = [
            r'total\s+cost\s+of\s+(?:sales|revenue)[^0-9]*(\d[\d,]+)',
            r'cost\s+of\s+(?:sales|revenue|goods\s+sold)[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=500, max_val=400000)
    
    def _extract_gross_profit(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract gross profit."""
        patterns = [
            r'\bgross\s+(?:profit|margin)[^0-9]*(\d[\d,]+)',
            r'total\s+gross\s+profit[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=500, max_val=200000)
    
    def _extract_rd_expense(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract R&D expense."""
        patterns = [
            r'research\s+and\s+development[^0-9]*(\d[\d,]+)',
            r'r\s*&\s*d\s+expense[s]?[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=100, max_val=50000)
    
    def _extract_sga_expense(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract Selling, General & Administrative expense."""
        patterns = [
            r'selling,?\s*general\s+and\s+administrative[^0-9]*(\d[\d,]+)',
            r'sg\s*&\s*a[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=100, max_val=50000)
    
    def _extract_depreciation(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract Depreciation & Amortization."""
        patterns = [
            r'depreciation\s+and\s+amortization[^0-9]*(\d[\d,]+)',
            r'd\s*&\s*a[^0-9]*(\d[\d,]+)',
            r'depreciation[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=100, max_val=30000)
    
    def _extract_operating_cash_flow(self, text: str, unit_scale: str) -> Optional[float]:
        """Extract cash from operating activities."""
        patterns = [
            r'cash\s+(?:generated\s+by|provided\s+by|from)\s+operating\s+activities[^0-9]*(\d[\d,]+)',
            r'operating\s+cash\s+flow[^0-9]*(\d[\d,]+)',
            r'net\s+cash\s+from\s+operations[^0-9]*(\d[\d,]+)',
        ]
        return self._extract_with_patterns(patterns, text, unit_scale, min_val=500, max_val=100000)
    
    def _extract_with_patterns(
        self, 
        patterns: List[str], 
        text: str, 
        unit_scale: str,
        min_val: float = 0,
        max_val: float = 1000000
    ) -> Optional[float]:
        """Generic pattern extraction helper."""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    value = self._normalize_to_millions(value, unit_scale)
                    
                    if min_val <= value <= max_val:
                        return value
                except (ValueError, IndexError):
                    continue
        return None
