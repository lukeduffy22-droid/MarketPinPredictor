"""
GEX Invariant Tests

These tests verify the core GEX invariants that MUST hold:
1. gross_gex >= abs(net_gex) always
2. gross_gex == call_gex_total + put_gex_total
3. net_gex == call_gex_total - put_gex_total
4. strike_count >= min_strikes for "valid" snapshots

These tests prevent regressions if anyone reintroduces netting-before-separating
or UI-style scaling in backend.
"""
import pytest
from app.core.gex import compute_gex, compute_aggregate_gex, GexResult, AggregateGexResult


class TestGexInvariants:
    """Test core GEX computation invariants."""
    
    def test_compute_gex_invariant_total_gte_abs_net(self):
        """Invariant: total_gex >= abs(net_gex)."""
        result = compute_gex(
            gamma_call=0.05,
            oi_call=1000,
            gamma_put=0.03,
            oi_put=800,
            multiplier=100
        )
        
        assert result.total_gex >= abs(result.net_gex), \
            f"Invariant violated: total_gex={result.total_gex} < abs(net_gex)={abs(result.net_gex)}"
    
    def test_compute_gex_balanced_exposure(self):
        """Test with equal call/put exposure."""
        result = compute_gex(
            gamma_call=0.05,
            oi_call=1000,
            gamma_put=0.05,
            oi_put=1000,
            multiplier=100
        )
        
        assert result.call_gex == result.put_gex
        assert result.net_gex == 0.0
        assert result.total_gex == 2 * result.call_gex
        assert result.total_gex >= abs(result.net_gex)
    
    def test_compute_gex_call_dominant(self):
        """Test with call-dominant exposure."""
        result = compute_gex(
            gamma_call=0.10,
            oi_call=2000,
            gamma_put=0.05,
            oi_put=500,
            multiplier=100
        )
        
        assert result.call_gex > result.put_gex
        assert result.net_gex > 0  # Positive = call dominant
        assert result.total_gex >= abs(result.net_gex)
    
    def test_compute_gex_put_dominant(self):
        """Test with put-dominant exposure."""
        result = compute_gex(
            gamma_call=0.02,
            oi_call=100,
            gamma_put=0.10,
            oi_put=5000,
            multiplier=100
        )
        
        assert result.put_gex > result.call_gex
        assert result.net_gex < 0  # Negative = put dominant
        assert result.total_gex >= abs(result.net_gex)
    
    def test_compute_gex_zero_input(self):
        """Test with zero inputs."""
        result = compute_gex(
            gamma_call=0,
            oi_call=0,
            gamma_put=0,
            oi_put=0,
            multiplier=100
        )
        
        assert result.call_gex == 0
        assert result.put_gex == 0
        assert result.net_gex == 0
        assert result.total_gex == 0
        assert result.total_gex >= abs(result.net_gex)


class TestAggregateGexInvariants:
    """Test aggregate GEX computation invariants."""
    
    def test_aggregate_gex_invariant_gross_gte_abs_net(self):
        """Invariant: gross_gex >= abs(net_gex)."""
        strikes_data = [
            {'call_gex': 100, 'put_gex': 50, 'net_gex': 50},
            {'call_gex': 200, 'put_gex': 100, 'net_gex': 100},
            {'call_gex': 50, 'put_gex': 150, 'net_gex': -100},
        ]
        
        result = compute_aggregate_gex(strikes_data)
        
        assert result.gross_gex >= abs(result.net_gex), \
            f"Invariant violated: gross_gex={result.gross_gex} < abs(net_gex)={abs(result.net_gex)}"
    
    def test_aggregate_gex_gross_equals_call_plus_put(self):
        """Invariant: gross_gex == call_gex_total + put_gex_total."""
        strikes_data = [
            {'call_gex': 100, 'put_gex': 50, 'net_gex': 50},
            {'call_gex': 200, 'put_gex': 100, 'net_gex': 100},
            {'call_gex': 50, 'put_gex': 150, 'net_gex': -100},
        ]
        
        result = compute_aggregate_gex(strikes_data)
        
        assert result.gross_gex == result.call_gex_total + result.put_gex_total, \
            f"Invariant violated: gross_gex={result.gross_gex} != call+put={result.call_gex_total + result.put_gex_total}"
    
    def test_aggregate_gex_net_equals_call_minus_put(self):
        """Invariant: net_gex == call_gex_total - put_gex_total."""
        strikes_data = [
            {'call_gex': 100, 'put_gex': 50, 'net_gex': 50},
            {'call_gex': 200, 'put_gex': 100, 'net_gex': 100},
            {'call_gex': 50, 'put_gex': 150, 'net_gex': -100},
        ]
        
        result = compute_aggregate_gex(strikes_data)
        
        assert result.net_gex == result.call_gex_total - result.put_gex_total, \
            f"Invariant violated: net_gex={result.net_gex} != call-put={result.call_gex_total - result.put_gex_total}"
    
    def test_aggregate_gex_strike_count(self):
        """Test strike count is accurate."""
        strikes_data = [
            {'call_gex': 100, 'put_gex': 50, 'net_gex': 50},
            {'call_gex': 200, 'put_gex': 100, 'net_gex': 100},
        ]
        
        result = compute_aggregate_gex(strikes_data)
        
        assert result.strike_count == 2, \
            f"Strike count wrong: expected 2, got {result.strike_count}"
    
    def test_aggregate_gex_empty_input(self):
        """Test with empty input returns zero result."""
        result = compute_aggregate_gex([])
        
        assert result.call_gex_total == 0
        assert result.put_gex_total == 0
        assert result.gross_gex == 0
        assert result.net_gex == 0
        assert result.strike_count == 0
        assert result.gross_gex >= abs(result.net_gex)


class TestGexEdgeCases:
    """Test edge cases for GEX computation."""
    
    def test_compute_gex_negative_gamma_raises(self):
        """Negative gamma should raise ValueError."""
        with pytest.raises(ValueError):
            compute_gex(
                gamma_call=-0.05,
                oi_call=1000,
                gamma_put=0.03,
                oi_put=800,
                multiplier=100
            )
    
    def test_compute_gex_negative_oi_raises(self):
        """Negative OI should raise ValueError."""
        with pytest.raises(ValueError):
            compute_gex(
                gamma_call=0.05,
                oi_call=-1000,
                gamma_put=0.03,
                oi_put=800,
                multiplier=100
            )
    
    def test_compute_gex_none_input_raises(self):
        """None input should raise ValueError."""
        with pytest.raises(ValueError):
            compute_gex(
                gamma_call=None,
                oi_call=1000,
                gamma_put=0.03,
                oi_put=800,
                multiplier=100
            )
    
    def test_aggregate_gex_large_values(self):
        """Test with large values (billions)."""
        strikes_data = [
            {'call_gex': 1e9, 'put_gex': 5e8, 'net_gex': 5e8},
            {'call_gex': 2e9, 'put_gex': 1e9, 'net_gex': 1e9},
        ]
        
        result = compute_aggregate_gex(strikes_data)
        
        assert result.gross_gex >= abs(result.net_gex)
        assert result.gross_gex == result.call_gex_total + result.put_gex_total
        assert result.net_gex == result.call_gex_total - result.put_gex_total
