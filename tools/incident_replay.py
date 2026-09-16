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
    arguments = parser.parse_args()
    if arguments.command == "export":
        result = export_incident_package(
            arguments.sources,
            destination=arguments.destination,
            project_root=arguments.project_root,
        )
    else:
        result = replay_incident_package(arguments.package)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
