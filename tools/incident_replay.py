from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.incident_replay import export_incident_package, replay_incident_package


def main() -> int:
    parser = argparse.ArgumentParser(description="Export or replay sanitized incident evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)
    export = subparsers.add_parser("export")
    export.add_argument("sources", nargs="+")
    export.add_argument("--destination", required=True)
    export.add_argument("--project-root", default=PROJECT_ROOT)
    replay = subparsers.add_parser("replay")
    replay.add_argument("package")
    opening = subparsers.add_parser("import-opening", help="Import retained opening failure NDJSON")
    opening.add_argument("source", type=Path)
    opening.add_argument("--destination", required=True, type=Path)
    opening.add_argument("--examples-per-group", type=int, default=2)
    arguments = parser.parse_args()
    if arguments.command == "export":
        result = export_incident_package(
            arguments.sources,
            destination=arguments.destination,
            project_root=arguments.project_root,
        )
    elif arguments.command == "import-opening":
        from backend.opening_incident_import import import_opening_failures
        result = import_opening_failures(
            arguments.source, destination=arguments.destination, project_root=PROJECT_ROOT,
            examples_per_group=arguments.examples_per_group)
    else:
        result = replay_incident_package(arguments.package)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
