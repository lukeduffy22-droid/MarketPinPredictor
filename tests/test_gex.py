"""
Unit tests for canonical GEX computation module.
Tests the invariants and sign conventions required for correct gamma exposure calculation.
"""
import pytest
from app.core.gex import (
    compute_gex, GexResult, validate_gex_invariant, assert_gex_invariant,
    compute_aggregate_gex, compute_aggregate_gex_from_arrays, AggregateGexResult
)


class TestComputeGex:
    """Tests for the compute_gex function."""
    
    def test_equal_oi_and_gamma_net_zero(self):
        """When oi_call == oi_put and gamma_call == gamma_put: net_gex == 0, total_gex > 0."""
        result = compute_gex(
            gamma_call=0.01,
            oi_call=1000,
            gamma_put=0.01,
            oi_put=1000,
            multiplier=100
        )
        
        # Net should be zero (perfect cancellation)
        assert abs(result.net_gex) < 1e-9, f"Expected net_gex near 0, got {result.net_gex}"
        
        # Total should be positive
        assert result.total_gex > 0, f"Expected total_gex > 0, got {result.total_gex}"
        
        # Invariant must hold
        assert result.total_gex >= abs(result.net_gex)
    
    def test_calls_dominate_positive_net(self):
        """When calls dominate: net_gex > 0."""
        result = compute_gex(
            gamma_call=0.02,
            oi_call=2000,
            gamma_put=0.01,
            oi_put=1000,
            multiplier=100
        )
        
        assert result.net_gex > 0, f"Expected positive net_gex, got {result.net_gex}"
        assert result.total_gex >= abs(result.net_gex)
    
    def test_puts_dominate_negative_net(self):
        """When puts dominate: net_gex < 0."""
        result = compute_gex(
            gamma_call=0.01,
            oi_call=1000,
            gamma_put=0.02,
            oi_put=2000,
            multiplier=100
        )
        
        assert result.net_gex < 0, f"Expected negative net_gex, got {result.net_gex}"
        assert result.total_gex >= abs(result.net_gex)
    
    def test_invariant_always_holds(self):
        """Invariant total_gex >= |net_gex| must always hold."""
        test_cases = [
            (0.01, 1000, 0.01, 1000),
            (0.02, 2000, 0.01, 1000),
            (0.01, 1000, 0.02, 2000),
            (0.05, 5000, 0.001, 100),
            (0.001, 100, 0.05, 5000),
            (0.0, 0, 0.01, 1000),  # Only puts
            (0.01, 1000, 0.0, 0),  # Only calls
        ]
        
        for gamma_call, oi_call, gamma_put, oi_put in test_cases:
            result = compute_gex(
                gamma_call=gamma_call,
                oi_call=oi_call,
                gamma_put=gamma_put,
                oi_put=oi_put,
                multiplier=100
            )
            
            assert result.total_gex >= abs(result.net_gex) - 1e-9, (
                f"Invariant violated for inputs: "
                f"gamma_call={gamma_call}, oi_call={oi_call}, "
                f"gamma_put={gamma_put}, oi_put={oi_put}. "
                f"Got total_gex={result.total_gex}, net_gex={result.net_gex}"
            )
    
    def test_call_gex_and_put_gex_components(self):
        """Verify individual call_gex and put_gex components."""
        result = compute_gex(
            gamma_call=0.01,
            oi_call=1000,
            gamma_put=0.02,
            oi_put=500,
            multiplier=100
        )
        
        expected_call_gex = 0.01 * 1000 * 100
        expected_put_gex = 0.02 * 500 * 100
        
        assert abs(result.call_gex - expected_call_gex) < 1e-9
        assert abs(result.put_gex - expected_put_gex) < 1e-9
        assert abs(result.net_gex - (expected_call_gex - expected_put_gex)) < 1e-9
        assert abs(result.total_gex - (expected_call_gex + expected_put_gex)) < 1e-9
    
    def test_zero_oi_returns_zero_gex(self):
        """Zero OI should result in zero GEX for that side."""
        result = compute_gex(
            gamma_call=0.01,
            oi_call=0,
            gamma_put=0.01,
            oi_put=1000,
            multiplier=100
        )
        
        # call_gex = 0.01 * 0 * 100 = 0
        assert result.call_gex == 0
        # put_gex = 0.01 * 1000 * 100 = 1000
        assert result.put_gex == 1000
        # net_gex = 0 - 1000 = -1000
        assert result.net_gex == -1000
        # total_gex = 0 + 1000 = 1000
        assert result.total_gex == 1000


class TestInputValidation:
    """Tests for input validation in compute_gex."""
    
    def test_negative_gamma_raises(self):
        """Negative gamma should raise ValueError."""
        with pytest.raises(ValueError, match="unsigned magnitudes"):
            compute_gex(
                gamma_call=-0.01,
                oi_call=1000,
                gamma_put=0.01,
                oi_put=1000
            )
    
    def test_negative_oi_raises(self):
        """Negative OI should raise ValueError."""
        with pytest.raises(ValueError, match="Open interest must be >= 0"):
            compute_gex(
                gamma_call=0.01,
                oi_call=-1000,
                gamma_put=0.01,
                oi_put=1000
            )
    
    def test_none_input_raises(self):
        """None inputs should raise ValueError."""
        with pytest.raises(ValueError, match="is None"):
            compute_gex(
                gamma_call=None,
                oi_call=1000,
                gamma_put=0.01,
                oi_put=1000
            )
    
    def test_non_numeric_raises(self):
        """Non-numeric inputs should raise TypeError."""
        with pytest.raises(TypeError, match="must be numeric"):
            compute_gex(
                gamma_call="0.01",
                oi_call=1000,
                gamma_put=0.01,
                oi_put=1000
            )


class TestValidateGexInvariant:
    """Tests for the validate_gex_invariant function."""
    
    def test_valid_invariant_returns_true(self):
        """Valid invariant should return True."""
        assert validate_gex_invariant(net_gex=5.0, total_gex=10.0) is True
        assert validate_gex_invariant(net_gex=-5.0, total_gex=10.0) is True
        assert validate_gex_invariant(net_gex=0.0, total_gex=10.0) is True
        assert validate_gex_invariant(net_gex=10.0, total_gex=10.0) is True
    
    def test_invalid_invariant_returns_false(self):
        """Invalid invariant should return False."""
        assert validate_gex_invariant(net_gex=15.0, total_gex=10.0) is False
        assert validate_gex_invariant(net_gex=-15.0, total_gex=10.0) is False


class TestAssertGexInvariant:
    """Tests for the assert_gex_invariant function."""
    
    def test_valid_invariant_does_not_raise(self):
        """Valid invariant should not raise."""
        assert_gex_invariant(net_gex=5.0, total_gex=10.0)
        assert_gex_invariant(net_gex=-5.0, total_gex=10.0)
    
    def test_invalid_invariant_raises(self):
        """Invalid invariant should raise AssertionError."""
        with pytest.raises(AssertionError, match="GEX invariant violated"):
            assert_gex_invariant(net_gex=15.0, total_gex=10.0)


class TestAggregateGex:
    """Tests for aggregate GEX functions - TOTAL_GEX_ABS and TOTAL_GEX_NET definitions."""
    
    def test_aggregate_gex_definition_abs(self):
        """TOTAL_GEX_ABS = sum(abs(net_gex_per_strike))."""
        strikes = [
            {'strike': 100, 'net_gex': 10.0},
            {'strike': 105, 'net_gex': -5.0},
            {'strike': 110, 'net_gex': 3.0},
        ]
        result = compute_aggregate_gex(strikes)
        expected_abs = abs(10.0) + abs(-5.0) + abs(3.0)  # 18.0
        assert result.total_gex_abs == expected_abs
    
    def test_aggregate_gex_definition_net(self):
        """TOTAL_GEX_NET = sum(net_gex_per_strike)."""
        strikes = [
            {'strike': 100, 'net_gex': 10.0},
            {'strike': 105, 'net_gex': -5.0},
            {'strike': 110, 'net_gex': 3.0},
        ]
        result = compute_aggregate_gex(strikes)
        expected_net = 10.0 + (-5.0) + 3.0  # 8.0
        assert result.total_gex_net == expected_net
    
    def test_aggregate_gex_strike_count(self):
        """Aggregate result includes strike count."""
        strikes = [
            {'strike': 100, 'net_gex': 10.0},
            {'strike': 105, 'net_gex': -5.0},
        ]
        result = compute_aggregate_gex(strikes)
        assert result.strike_count == 2
    
    def test_aggregate_gex_empty_list(self):
        """Empty list returns zero aggregates."""
        result = compute_aggregate_gex([])
        assert result.total_gex_abs == 0.0
        assert result.total_gex_net == 0.0
        assert result.strike_count == 0
    
    def test_aggregate_gex_from_arrays(self):
        """compute_aggregate_gex_from_arrays works with simple list."""
        net_gex_values = [10.0, -5.0, 3.0]
        result = compute_aggregate_gex_from_arrays(net_gex_values)
        assert result.total_gex_abs == 18.0
        assert result.total_gex_net == 8.0
        assert result.strike_count == 3
    
    def test_aggregate_gex_all_negative(self):
        """All negative net_gex values sum correctly."""
        strikes = [
            {'strike': 100, 'net_gex': -10.0},
            {'strike': 105, 'net_gex': -5.0},
        ]
        result = compute_aggregate_gex(strikes)
        assert result.total_gex_abs == 15.0
        assert result.total_gex_net == -15.0
    
    def test_aggregate_gex_invalid_input_not_list(self):
        """Non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            compute_aggregate_gex("not a list")
    
    def test_aggregate_gex_missing_net_gex_key(self):
        """Missing net_gex key raises ValueError."""
        strikes = [{'strike': 100}]  # Missing 'net_gex'
        with pytest.raises(ValueError, match="net_gex"):
            compute_aggregate_gex(strikes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
