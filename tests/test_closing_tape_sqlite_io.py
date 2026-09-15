import sqlite3

import pytest

from backend.closing_tape.dataset import _read_only
from backend.closing_tape.readiness import _connect
from backend.closing_tape.sqlite_io import sqlite_read_only_uri


@pytest.mark.parametrize("connect", (_connect, _read_only))
def test_read_only_catalog_uri_escapes_fragment_characters(tmp_path, connect):
    target = tmp_path / "archive#copy" / "closing_tape.sqlite"
    target.parent.mkdir()
    wrong_sibling = tmp_path / "archive"
    with sqlite3.connect(target) as connection:
        connection.execute("CREATE TABLE expected (value INTEGER)")
    with sqlite3.connect(wrong_sibling) as connection:
        connection.execute("CREATE TABLE wrong (value INTEGER)")

    uri = sqlite_read_only_uri(target)
    assert "%23" in uri
    with connect(target) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    assert tables == {"expected"}
