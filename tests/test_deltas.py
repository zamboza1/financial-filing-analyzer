"""Tests for delta calculation."""

import pytest
from backend.entities import KpiSnapshot
from backend.deltas import compare_kpis, DeltaItem, format_delta_summary, _format_value


class TestDeltaItem:
    """Test DeltaItem dataclass."""
    
    def test_calculate_delta(self):
        """Test that delta is calculated correctly."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=100.0,
            previous_value=90.0
        )
        
        assert delta.delta == 10.0
        assert delta.pct_change is not None
        assert abs(delta.pct_change - 11.11) < 0.1  # ~11.11% increase
    
    def test_negative_delta(self):
        """Test negative delta calculation."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=80.0,
            previous_value=100.0
        )
        
        assert delta.delta == -20.0
        assert delta.pct_change is not None
        assert abs(delta.pct_change - (-20.0)) < 0.1
    
    def test_zero_previous_value(self):
        """Test handling of zero previous value."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=100.0,
            previous_value=0.0
        )
        
        assert delta.delta == 100.0
        # Percentage change should be infinite (or handled specially)
        assert delta.pct_change == float('inf')
    
    def test_both_zero(self):
        """Test when both values are zero."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=0.0,
            previous_value=0.0
        )
        
        assert delta.delta == 0.0
        assert delta.pct_change == 0.0
    
    def test_missing_current_value(self):
        """Test when current value is None."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=None,
            previous_value=100.0
        )
        
        assert delta.delta is None
        assert delta.pct_change is None
    
    def test_missing_previous_value(self):
        """Test when previous value is None."""
        delta = DeltaItem(
            metric_name="Revenue",
            current_value=100.0,
            previous_value=None
        )
        
        assert delta.delta is None
        assert delta.pct_change is None


class TestCompareKPIs:
    """Test compare_kpis function."""
    
    def test_compare_partial_metrics(self):
        """Test comparing snapshots with only some metrics."""
        current = KpiSnapshot(
            period_end="2023-09-30",
            revenue=89587.0,
            net_income=22956.0
        )
        
        previous = KpiSnapshot(
            period_end="2023-06-30",
            revenue=81797.0,
            eps=1.26
        )
        
        deltas = compare_kpis(current, previous)
        
        # Should have deltas for revenue, net_income, and eps
        assert len(deltas) >= 3
        
        revenue_delta = next(d for d in deltas if d.metric_name == "Revenue")
        assert revenue_delta.current_value == 89587.0
        assert revenue_delta.previous_value == 81797.0
        
        net_income_delta = next(d for d in deltas if d.metric_name == "Net Income")
        assert net_income_delta.current_value == 22956.0
        assert net_income_delta.previous_value is None
    
    def test_compare_missing_previous(self):
        """Test when previous snapshot has no data."""
        # Need at least one field for KpiSnapshot validation
        # Use guidance as the one field
        current = KpiSnapshot(
            period_end="2023-09-30",
            revenue=89587.0
        )
        
        previous = KpiSnapshot(
            period_end="2023-06-30",
            guidance="No guidance provided"
        )
        
        deltas = compare_kpis(current, previous)
        
        revenue_delta = next(d for d in deltas if d.metric_name == "Revenue")
        assert revenue_delta.current_value == 89587.0
        assert revenue_delta.previous_value is None
        assert revenue_delta.delta is None


class TestFormatDeltaSummary:
    """Test format_delta_summary function."""
    
    def test_format_with_changes(self):
        """Test formatting deltas with changes."""
        deltas = [
            DeltaItem("Revenue", 100.0, 90.0),
            DeltaItem("Net Income", 20.0, 18.0),
        ]
        
        summary = format_delta_summary(deltas)
        
        assert "Revenue" in summary
        assert "Net Income" in summary
        assert "%" in summary  # Should have percentage changes
    
    def test_format_with_new_metric(self):
        """Test formatting when metric is new."""
        deltas = [
            DeltaItem("Revenue", 100.0, None),
        ]
        
        summary = format_delta_summary(deltas)
        
        assert "Revenue" in summary
        assert "new" in summary.lower()
    
    def test_format_with_missing_metric(self):
        """Test formatting when metric is missing."""
        deltas = [
            DeltaItem("Revenue", None, 90.0),
        ]
        
        summary = format_delta_summary(deltas)
        
        assert "Revenue" in summary
        assert "Not reported" in summary or "was" in summary.lower()


class TestFormatValue:
    """Test _format_value helper function."""
    
    def test_format_margin(self):
        """Test formatting margin values."""
        result = _format_value(0.371, "Gross Margin")
        assert "%" in result
        assert "37.1" in result
    
    def test_format_eps(self):
        """Test formatting EPS values."""
        result = _format_value(1.46, "EPS")
        assert "$" in result
        assert "1.46" in result
    
    def test_format_large_revenue(self):
        """Test formatting large revenue (billions)."""
        result = _format_value(89587.0, "Revenue")
        assert "$" in result
        assert "B" in result or "M" in result  # Should show billions or millions
    
    def test_format_small_revenue(self):
        """Test formatting small revenue (millions)."""
        result = _format_value(500.0, "Revenue")
        assert "$" in result
        assert "M" in result
