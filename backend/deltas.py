"""Period-over-period KPI comparison and delta calculation."""

from typing import List, Optional
from dataclasses import dataclass
from backend.entities import KpiSnapshot


@dataclass
class DeltaItem:
    """
    Represents a change in a KPI between two periods.
    
    Representation Invariants:
    - metric_name is non-empty
    - At least one of current_value or previous_value is not None
    - If both values exist, delta and pct_change are calculated
    """
    metric_name: str
    current_value: Optional[float]
    previous_value: Optional[float]
    delta: Optional[float] = None  # Absolute change
    pct_change: Optional[float] = None  # Percentage change
    
    def __post_init__(self) -> None:
        """Calculate delta and percentage change if both values exist."""
        if self.current_value is not None and self.previous_value is not None:
            self.delta = self.current_value - self.previous_value
            
            # Calculate percentage change (avoid division by zero)
            if self.previous_value != 0:
                self.pct_change = (self.delta / abs(self.previous_value)) * 100.0
            elif self.current_value != 0:
                # Previous was zero, current is not - infinite change
                self.pct_change = float('inf') if self.current_value > 0 else float('-inf')
            else:
                self.pct_change = 0.0


def compare_kpis(current: KpiSnapshot, previous: KpiSnapshot) -> List[DeltaItem]:
    """
    Compare two KPI snapshots and generate delta items.
    
    Preconditions:
    - current and previous are valid KpiSnapshot objects
    - At least one KPI is populated in each snapshot
    
    Postconditions:
    - Returns list of DeltaItem objects for all KPIs that exist in either snapshot
    - Deltas are calculated where both values exist
    - Percentage changes are calculated when previous != 0
    
    Args:
        current: Current period KPI snapshot
        previous: Previous period KPI snapshot
        
    Returns:
        List of DeltaItem objects showing changes
    """
    deltas: List[DeltaItem] = []
    
    # Compare each metric
    # Format: (attribute_name, display_name)
    metrics = [
        # Core income statement
        ('revenue', 'Revenue'),
        ('cost_of_revenue', 'Cost of Revenue'),
        ('gross_profit', 'Gross Profit'),
        ('operating_income', 'Operating Income'),
        ('net_income', 'Net Income'),
        ('eps', 'EPS'),
        # Profitability ratios
        ('gross_margin', 'Gross Margin'),
        ('operating_margin', 'Operating Margin'),
        ('net_margin', 'Net Margin'),
        # EBITDA
        ('ebitda', 'EBITDA'),
        # Cash flow
        ('operating_cash_flow', 'Operating Cash Flow'),
        ('free_cash_flow', 'Free Cash Flow'),
        # Expenses
        ('research_and_development', 'R&D Expense'),
        ('selling_general_admin', 'SG&A Expense'),
        ('depreciation_amortization', 'D&A'),
    ]
    
    for attr_name, display_name in metrics:
        current_val = getattr(current, attr_name, None)
        previous_val = getattr(previous, attr_name, None)
        
        # Only include if at least one value exists
        if current_val is not None or previous_val is not None:
            delta = DeltaItem(
                metric_name=display_name,
                current_value=current_val,
                previous_value=previous_val
            )
            deltas.append(delta)
    
    return deltas


def format_delta_summary(deltas: List[DeltaItem]) -> str:
    """
    Format delta items into a human-readable summary.
    
    Args:
        deltas: List of DeltaItem objects
        
    Returns:
        Formatted summary string with bullet points
    """
    if not deltas:
        return "No changes detected."
    
    summary_lines = []
    
    for delta in deltas:
        if delta.current_value is None and delta.previous_value is None:
            continue
        
        if delta.current_value is None:
            summary_lines.append(f"- **{delta.metric_name}**: Not reported (was {_format_value(delta.previous_value, delta.metric_name)})")
        elif delta.previous_value is None:
            summary_lines.append(f"- **{delta.metric_name}**: {_format_value(delta.current_value, delta.metric_name)} (new)")
        else:
            # Both values exist
            change_str = ""
            if delta.pct_change is not None:
                if abs(delta.pct_change) == float('inf'):
                    change_str = " (new)"
                else:
                    sign = "+" if delta.pct_change >= 0 else ""
                    change_str = f" ({sign}{delta.pct_change:.1f}%)"
            
            delta_str = ""
            if delta.delta is not None:
                sign = "+" if delta.delta >= 0 else ""
                delta_str = f" ({sign}{_format_value(delta.delta, delta.metric_name)})"
            
            summary_lines.append(
                f"- **{delta.metric_name}**: {_format_value(delta.current_value, delta.metric_name)} "
                f"vs {_format_value(delta.previous_value, delta.metric_name)}{change_str}{delta_str}"
            )
    
    return "\n".join(summary_lines)


def _format_value(value: float, metric_name: str) -> str:
    """
    Format a value for display based on metric type.
    
    Args:
        value: Numeric value
        metric_name: Name of the metric (for context)
        
    Returns:
        Formatted string
    """
    if 'Margin' in metric_name:
        # Percentage
        return f"{value * 100:.1f}%"
    elif 'EPS' in metric_name:
        # Currency per share
        return f"${value:.2f}"
    else:
        # Monetary value in millions
        if abs(value) >= 1000:
            return f"${value/1000:.2f}B"
        else:
            return f"${value:.2f}M"
