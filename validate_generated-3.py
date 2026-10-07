#!/usr/bin/env python3
"""
T24 source validation used internally by run_producer.sh.

No command-line arguments are required.

Validation rules:
- One physical line = one CDC change/event.
- A file may contain many rows.
- Every row is validated independently.
- If ANY row is invalid, the WHOLE FILE is marked CORRUPT.
- Only XMLRECORD is required.
- All other fields are optional.
- XMLRECORD is opaque and is NOT parsed.
- Physical input layout:
    op_ts;optype;csn;capture_ts;xid;opseqno;pos;RECID;reserved;XMLRECORD
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

INPUT_ROOT = Path("/data/T24")
DEFINITIONS_FILE = BASE_DIR / "topic_definitions.json"
LOG_DIR = BASE_DIR / "logs"

EXPECTED_PHYSICAL_FIELDS = 10

IDX = {
    "op_ts": 0,
    "optype": 1,
    "csn": 2,
    "capture_ts": 3,
    "xid": 4,
    "opseqno": 5,
    "pos": 6,
    "RECID": 7,
    "reserved": 8,
    "XMLRECORD": 9,
}


def clean_capture_field(value: str) -> str:
    value = value.strip()

    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1].replace('""', '"')

    return value


def load_routes():
    with DEFINITIONS_FILE.open("r", encoding="utf-8") as f:
        doc = json.load(f)

    topics = doc.get("topics")

    if not isinstance(topics, dict):
        raise ValueError(
            "topic_definitions.json must contain an object named 'topics'"
        )

    routes = {}

    for topic, cfg in topics.items():
        cfg = cfg or {}

        if not isinstance(cfg, dict):
            raise ValueError(
                f"Invalid configuration for topic {topic!r}: expected object"
            )

        default_name = topic.split(".", 1)[1] if "." in topic else topic

        folder = (
            cfg.get("source_folder")
            or cfg.get("sourceFolder")
            or cfg.get("folder")
            or cfg.get("source")
            or default_name
        )

        table = (
            cfg.get("table")
            or cfg.get("table_name")
            or default_name
        )

        routes[str(folder)] = {
            "topic": str(topic),
            "table": str(table),
        }

    return routes


def find_route(path: Path, routes):
    for parent in [path.parent, *path.parents]:
        if parent.name in routes:
            return parent.name, routes[parent.name]

    return None, None


def add_issue(issues, line_no, code, field=None, detail=None):
    issues.append(
        {
            "line": line_no,
            "code": code,
            "field": field,
            "detail": detail,
        }
    )


def validate_file(path: Path):
    issues = []
    row_count = 0

    try:
        with path.open(
            "r",
            encoding="utf-8",
            errors="strict",
            newline=""
        ) as f:

            for line_no, raw in enumerate(f, start=1):
                raw = raw.rstrip("\r\n")

                if raw == "":
                    add_issue(
                        issues,
                        line_no,
                        "EMPTY_ROW",
                        detail="physical line is empty",
                    )
                    continue

                row_count += 1

                # Split only the first 9 delimiters so any semicolons inside
                # XMLRECORD remain part of XMLRECORD.
                parts = raw.split(";", 9)

                if len(parts) != EXPECTED_PHYSICAL_FIELDS:
                    add_issue(
                        issues,
                        line_no,
                        "BAD_PHYSICAL_LAYOUT",
                        detail=(
                            f"expected {EXPECTED_PHYSICAL_FIELDS} fields, "
                            f"found {len(parts)}"
                        ),
                    )
                    continue

                xmlrecord = clean_capture_field(parts[IDX["XMLRECORD"]])

                # Only XMLRECORD is mandatory.
                if xmlrecord == "":
                    add_issue(
                        issues,
                        line_no,
                        "MISSING_REQUIRED_FIELD",
                        "XMLRECORD",
                        "empty/null",
                    )

                # All other physical/logical fields are optional.
                # XMLRECORD is deliberately NOT parsed.

    except UnicodeDecodeError as exc:
        add_issue(
            issues,
            0,
            "INVALID_UTF8",
            detail=str(exc),
        )

    except OSError as exc:
        add_issue(
            issues,
            0,
            "FILE_READ_ERROR",
            detail=str(exc),
        )

    if row_count == 0:
        add_issue(
            issues,
            0,
            "NO_CHANGE_ROWS",
            detail="file contains no non-empty change rows",
        )

    return row_count, issues


def write_corruption_log(log_path: Path, corrupt_files):
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", encoding="utf-8") as out:
        out.write(
            "T24 corruption report generated "
            f"{datetime.now(timezone.utc).isoformat()}\n"
        )
        out.write("=" * 100 + "\n\n")

        for item in corrupt_files:
            out.write(f"CORRUPT file={item['file']}\n")
            out.write(f"  folder={item.get('folder') or 'UNKNOWN'}\n")
            out.write(f"  topic={item.get('topic') or 'UNKNOWN'}\n")
            out.write(
                f"  derived_table={item.get('table') or 'UNKNOWN'}\n"
            )
            out.write(f"  rows_seen={item['rows']}\n")

            for problem in item["issues"]:
                fields = [
                    f"line={problem['line']}",
                    f"code={problem['code']}",
                ]

                if problem.get("field"):
                    fields.append(f"field={problem['field']}")

                if problem.get("detail"):
                    fields.append(f"detail={problem['detail']}")

                out.write("  " + " ".join(fields) + "\n")

            out.write("\n")


def main():
    if not INPUT_ROOT.is_dir():
        print(
            f"ERROR: input root not found: {INPUT_ROOT}",
            file=sys.stderr,
        )
        return 2

    if not DEFINITIONS_FILE.is_file():
        print(
            f"ERROR: topic definitions not found: {DEFINITIONS_FILE}",
            file=sys.stderr,
        )
        return 2

    try:
        routes = load_routes()
    except Exception as exc:
        print(
            f"ERROR: cannot load topic definitions: {exc}",
            file=sys.stderr,
        )
        return 2

    files = sorted(INPUT_ROOT.rglob("*.txt"))

    total_files = 0
    valid_files = 0
    total_rows = 0
    corrupt_files = []

    for path in files:
        total_files += 1

        folder, route = find_route(path, routes)
        rows, issues = validate_file(path)
        total_rows += rows

        if route is None:
            issues.append(
                {
                    "line": 0,
                    "code": "UNKNOWN_SOURCE_FOLDER",
                    "field": None,
                    "detail": (
                        "file is not under a configured source folder"
                    ),
                }
            )

        if issues:
            corrupt_files.append(
                {
                    "file": str(path),
                    "folder": folder,
                    "topic": (
                        route.get("topic") if route else None
                    ),
                    "table": (
                        route.get("table") if route else None
                    ),
                    "rows": rows,
                    "issues": issues,
                }
            )
        else:
            valid_files += 1

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    log_path = LOG_DIR / f"corrupt_files_{timestamp}.log"

    if corrupt_files:
        write_corruption_log(log_path, corrupt_files)

    print("============================================================")
    print("T24 VALIDATION SUMMARY")
    print("============================================================")
    print(f"Input root      : {INPUT_ROOT}")
    print(f"Files scanned   : {total_files}")
    print(f"Valid files     : {valid_files}")
    print(f"Corrupt files   : {len(corrupt_files)}")
    print(f"Change rows seen: {total_rows}")

    if corrupt_files:
        print(f"Corruption log  : {log_path}")
        print("VALIDATION FAILED")
        print("Kafka producer will not start.")
        return 1

    print("VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
