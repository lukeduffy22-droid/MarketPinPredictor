from io import StringIO

import pandas as pd
import pytest

from app.utils.gamma_wall_evidence import normalize_gamma_walls


@pytest.mark.parametrize("put,count,expected", [(0, 0, None), (0, None, None), (0, 1, 0), (30, 1, 30)])
def test_missing_and_calculated_zero_remain_distinct_in_display_and_csv(put, count, expected):
    source = pd.DataFrame([{
        "strike": 29000, "call_gex": 44, "put_gex": put,
        "call_calculated_contracts": 1, "put_calculated_contracts": count,
        "gex": 44 - put,
    }])
    original = source.copy(deep=True)
    result = normalize_gamma_walls(source)
    exported = pd.read_csv(StringIO(result.to_csv(index=False)))
    pd.testing.assert_frame_equal(source, original)
    pd.testing.assert_frame_equal(normalize_gamma_walls(result), result)
    for frame in (result, exported):
        if expected is None:
            assert pd.isna(frame.loc[0, "put_gex"])
            assert pd.isna(frame.loc[0, "total_gex"])
        else:
            assert frame.loc[0, "put_gex"] == expected
            assert frame.loc[0, "total_gex"] == 44 + expected
        assert pd.isna(frame.loc[0, "days_to_expiry"])
        assert pd.isna(frame.loc[0, "expiration_count"])


def test_net_alone_cannot_establish_gross_or_side_coverage():
    result = normalize_gamma_walls(pd.DataFrame([{"net_gex": -20}]))
    assert result.loc[0, "net_gex"] == -20
    assert pd.isna(result.loc[0, "total_gex"])
    assert result.loc[0, "coverage"] == "partial_or_unknown"


def test_gross_is_not_used_as_a_net_alias():
    result = normalize_gamma_walls(pd.DataFrame([{"total_gex": 100}]))
    assert "net_gex" not in result
