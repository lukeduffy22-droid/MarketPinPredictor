from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.historical_import import (
    describe_historical_import,
    import_historical_bundle,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify or import one attested Databento historical bundle into "
            "MarketPin's separate observed and inferred evidence tables"
        )
    )
    parser.add_argument("manifest", help="path to manifest.v2.json")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--catalog-path")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the guarded decompression, replay, and catalog import",
    )
    args = parser.parse_args(argv)

    project_root = Path(args.project_root).resolve()
    manifest_path = Path(args.manifest).resolve()
    try:
        if not args.execute:
            payload = describe_historical_import(
                project_root,
                manifest_path,
                catalog_path=args.catalog_path,
            )
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0 if payload["executable"] else 2

        result = import_historical_bundle(
            project_root,
            manifest_path,
            catalog_path=args.catalog_path,
        )
    except (
        FileNotFoundError,
        OSError,
        RuntimeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps(
                {"status": "refused", "error": f"{type(exc).__name__}: {exc}"},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
