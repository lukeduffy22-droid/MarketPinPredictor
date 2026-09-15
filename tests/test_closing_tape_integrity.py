from pathlib import Path

from backend.closing_tape.integrity import inspect_dbn


def test_probe_dbn_decodes_and_preserves_tcbbo_as_observed_data():
    sample = Path(__file__).parents[1] / ".codex_tmp" / "opra_probe_5d4993deeb574657a5908a74fe5fa7bd.dbn"
    if not sample.exists():
        import pytest

        pytest.skip("local OPRA probe is unavailable")

    report = inspect_dbn(sample, require_tcbbo=True)

    assert report.complete
    assert report.dataset == "OPRA.PILLAR"
    assert report.trade_records == 1250
    assert report.tcbbo_records == 1250
    assert report.tcbbo_timestamped_records == 1250
    assert report.tcbbo_valid_nbbo_records == 1243
    assert dict(report.tcbbo_action_counts) == {"T": 1250}
    assert report.tcbbo_flagged_records == 1250
    assert report.mapping_records == 20496
    assert report.subscription_acks >= 1
    assert not report.provider_errors
    assert len(report.sha256) == 64
