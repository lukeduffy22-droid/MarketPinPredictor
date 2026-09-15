from backend.streamer import _should_use_databento


def test_auto_prefers_databento_when_key_exists():
    assert _should_use_databento("auto", "polygon-key", "db-key") is True


def test_auto_uses_polygon_when_databento_key_missing():
    assert _should_use_databento("auto", "polygon-key", "") is False


def test_explicit_provider_modes_are_respected():
    assert _should_use_databento("databento", "polygon-key", "") is True
    assert _should_use_databento("polygon", "", "db-key") is False
