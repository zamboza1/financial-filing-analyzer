"""Markdown report generation."""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
from backend.entities import Company, KpiSnapshot, DocumentChunk
from backend.deltas import compare_kpis, format_delta_summary, DeltaItem


class ResearchReport:
    """
    Generates Markdown research reports from KPI data and evidence.
    
    Creates a structured report with KPIs, deltas, and cited evidence.
    Designed to look like a student equity research memo.
    
    Representation Invariants:
    - All evidence chunks are linked to source filings
    - KPI values are formatted consistently
    - Report is valid Markdown
    """
    
    def __init__(self, company: Company) -> None:
        """
        Initialize report generator for a company.
        
        Args:
            company: Company entity for the report
        """
        self._company = company
    
    def generate(
        self,
        current_snapshot: KpiSnapshot,
        previous_snapshot: KpiSnapshot,
        evidence_chunks: List[DocumentChunk],
        output_path: Optional[Path] = None
    ) -> str:
        """
        Generate a complete Markdown report.
        
        Preconditions:
        - current_snapshot and previous_snapshot are valid
        - evidence_chunks is a list of relevant chunks (typically from search)
        
        Postconditions:
        - Returns complete Markdown report as string
        - If output_path provided, saves report to file
        
        Args:
            current_snapshot: Current period KPIs
            previous_snapshot: Previous period KPIs
            evidence_chunks: List of evidence chunks to cite
            output_path: Optional path to save report
            
        Returns:
            Markdown report as string
        """
        # Generate sections
        header = self._generate_header()
        snapshot = self._generate_snapshot(current_snapshot)
        kpi_table = self._generate_kpi_table(current_snapshot, previous_snapshot)
        deltas = self._generate_deltas(current_snapshot, previous_snapshot)
        evidence = self._generate_evidence(evidence_chunks)
        
        # Combine sections
        report = "\n\n".join([
            header,
            snapshot,
            kpi_table,
            deltas,
            evidence
        ])
        
        # Save if path provided
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report, encoding='utf-8')
        
        return report
    
    def _generate_header(self) -> str:
        """Generate report header."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""# Equity Research Report: {self._company.name} ({self._company.ticker})

**Generated:** {timestamp}  
**CIK:** {self._company.cik}

---
"""
    
    def _generate_snapshot(self, snapshot: KpiSnapshot) -> str:
        """Generate snapshot section."""
        lines = ["## Snapshot", ""]
        lines.append(f"**Period End:** {snapshot.period_end}")
        lines.append("")
        lines.append("### Key Metrics")
        lines.append("")
        
        if snapshot.revenue is not None:
            lines.append(f"- **Revenue:** ${snapshot.revenue:,.2f}M")
        if snapshot.net_income is not None:
            lines.append(f"- **Net Income:** ${snapshot.net_income:,.2f}M")
        if snapshot.eps is not None:
            lines.append(f"- **EPS:** ${snapshot.eps:.2f}")
        if snapshot.operating_margin is not None:
            lines.append(f"- **Operating Margin:** {snapshot.operating_margin * 100:.1f}%")
        if snapshot.gross_margin is not None:
            lines.append(f"- **Gross Margin:** {snapshot.gross_margin * 100:.1f}%")
        
        return "\n".join(lines)
    
    def _generate_kpi_table(self, current: KpiSnapshot, previous: KpiSnapshot) -> str:
        """Generate KPI comparison table."""
        lines = ["## KPI Comparison", ""]
        lines.append("| Metric | Current | Previous | Change | % Change |")
        lines.append("|--------|---------|----------|--------|----------|")
        
        deltas = compare_kpis(current, previous)
        
        for delta in deltas:
            current_str = self._format_kpi_value(delta.current_value, delta.metric_name)
            previous_str = self._format_kpi_value(delta.previous_value, delta.metric_name)
            
            if delta.delta is not None:
                delta_str = self._format_kpi_value(delta.delta, delta.metric_name, show_sign=True)
            else:
                delta_str = "N/A"
            
            if delta.pct_change is not None:
                if abs(delta.pct_change) == float('inf'):
                    pct_str = "∞"
                else:
                    sign = "+" if delta.pct_change >= 0 else ""
                    pct_str = f"{sign}{delta.pct_change:.1f}%"
            else:
                pct_str = "N/A"
            
            lines.append(f"| {delta.metric_name} | {current_str} | {previous_str} | {delta_str} | {pct_str} |")
        
        return "\n".join(lines)
    
    def _generate_deltas(self, current: KpiSnapshot, previous: KpiSnapshot) -> str:
        """Generate 'What Changed' section."""
        lines = ["## What Changed", ""]
        deltas = compare_kpis(current, previous)
        summary = format_delta_summary(deltas)
        lines.append(summary)
        return "\n".join(lines)
    
    def _generate_evidence(self, chunks: List[DocumentChunk]) -> str:
        """Generate evidence section with cited snippets."""
        lines = ["## Evidence", ""]
        lines.append("Key excerpts from SEC filings supporting the analysis:")
        lines.append("")
        
        # Limit to 8 chunks max
        chunks = chunks[:8]
        
        for i, chunk in enumerate(chunks, 1):
            # Truncate text if too long
            text = chunk.text
            if len(text) > 300:
                text = text[:297] + "..."
            
            # Format citation
            filing = chunk.source_filing
            citation = f"{filing.company.ticker} {filing.filing_type} ({filing.period_end}), Chunk {chunk.chunk_index}"
            
            lines.append(f"### Evidence {i}")
            lines.append("")
            lines.append(f"**Source:** {citation}  ")
            lines.append(f"**Chunk ID:** `{chunk.chunk_id}`")
            lines.append("")
            lines.append("> " + text.replace("\n", "\n> "))
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_kpi_value(
        self,
        value: Optional[float],
        metric_name: str,
        show_sign: bool = False
    ) -> str:
        """
        Format a KPI value for display.
        
        Args:
            value: Numeric value (or None)
            metric_name: Name of metric
            show_sign: Whether to show + sign for positive values
            
        Returns:
            Formatted string
        """
        if value is None:
            return "N/A"
        
        if 'Margin' in metric_name:
            # Percentage
            sign = "+" if show_sign and value >= 0 else ""
            return f"{sign}{value * 100:.1f}%"
        elif 'EPS' in metric_name:
            # Currency per share
            sign = "+" if show_sign and value >= 0 else ""
            return f"{sign}${value:.2f}"
        else:
            # Monetary value in millions
            sign = "+" if show_sign and value >= 0 else ""
            if abs(value) >= 1000:
                return f"{sign}${value/1000:.2f}B"
            else:
                return f"{sign}${value:.2f}M"


def save_report(
    report: str,
    company: Company,
    base_path: Path
) -> Path:
    """
    Save report to file with timestamp.
    
    Args:
        report: Report content (Markdown string)
        company: Company entity
        base_path: Base directory for reports
        
    Returns:
        Path to saved report file
    """
    base_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{company.ticker}_{timestamp}.md"
    filepath = base_path / filename
    
    filepath.write_text(report, encoding='utf-8')
    return filepath
