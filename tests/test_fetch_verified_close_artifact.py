import hashlib
from datetime import date

import pytest

from tools.fetch_verified_close_artifact import fetch_official_artifact


class _Response:
    def __init__(self, body: bytes, *, url: str, content_type="text/csv", length=None):
        self.body = body
        self.url = url
        self.headers = {"Content-Type": content_type}
        if length is not None:
            self.headers["Content-Length"] = str(length)
        self.closed = False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), max(1, chunk_size)):
            yield self.body[offset:offset + chunk_size]

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_fetch_stages_exact_official_bytes_idempotently(tmp_path):
    body = b"DATE,OPEN,HIGH,LOW,CLOSE\n08/25/2026,15.71,15.90,15.40,15.45\n"
    response = _Response(
        body, url="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        length=len(body),
    )
    session = _Session(response)
    arguments = dict(
        symbol="VIX", source="cboe-official",
        source_reference="https://www.cboe.com/vix-history",
        trading_date=date(2026, 8, 25), project_root=tmp_path,
        http_session=session,
    )

    first = fetch_official_artifact(**arguments)
    response.closed = False
    second = fetch_official_artifact(**arguments)

    digest = hashlib.sha256(body).hexdigest()
    assert first["source_artifact_sha256"] == digest
    assert first["source_artifact_path"] == second["source_artifact_path"]
    assert first["final_url"].startswith("https://cdn.cboe.com/")
    assert (tmp_path / "data" / "verified_close_sources" / "2026-08-25" / "VIX" / f"{digest}.csv").read_bytes() == body
    assert response.closed


def test_fetch_rejects_redirect_outside_official_domain(tmp_path):
    response = _Response(b"not official", url="https://example.com/vix.csv")

    with pytest.raises(ValueError, match="official cboe.com domain"):
        fetch_official_artifact(
            symbol="VIX", source="cboe-official",
            source_reference="https://www.cboe.com/vix-history",
            trading_date=date(2026, 8, 25), project_root=tmp_path,
            http_session=_Session(response),
        )

    assert not list(tmp_path.rglob("*.csv"))
    assert response.closed


def test_fetch_enforces_streaming_size_limit_and_cleans_partial_file(tmp_path):
    response = _Response(b"123456", url="https://cdn.cboe.com/vix.csv")

    with pytest.raises(ValueError, match="size limit"):
        fetch_official_artifact(
            symbol="VIX", source="cboe-official",
            source_reference="https://www.cboe.com/vix-history",
            trading_date=date(2026, 8, 25), project_root=tmp_path,
            max_bytes=5, http_session=_Session(response),
        )

    assert not list(tmp_path.rglob(".download.*.tmp"))
    assert response.closed
