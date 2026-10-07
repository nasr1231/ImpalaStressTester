#!/usr/bin/env python3
import argparse, json, sys
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_PHYSICAL_FIELDS = 10
VALID_OPS = {"I","U","D","P"}
IDX = {
    "op_ts":0, "optype":1, "csn":2, "capture_ts":3, "xid":4,
    "opseqno":5, "pos":6, "RECID":7, "reserved":8, "XMLRECORD":9
}
REQUIRED_ROW_FIELDS = ("op_ts","optype","csn","pos","RECID","XMLRECORD")

def clean(v):
    v = v.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1].replace('""','"')
    return v

def load_routes(path):
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    topics = doc.get("topics")
    if not isinstance(topics, dict):
        raise ValueError("topic_definitions.json must contain object 'topics'")
    routes = {}
    for topic, cfg in topics.items():
        cfg = cfg or {}
        default = topic.split(".",1)[1] if "." in topic else topic
        folder = cfg.get("source_folder") or cfg.get("sourceFolder") or cfg.get("folder") or cfg.get("source") or default
        table = cfg.get("table") or cfg.get("table_name") or default
        routes[str(folder)] = {"topic": str(topic), "table": str(table)}
    return routes

def find_route(path, routes):
    for p in [path.parent, *path.parents]:
        if p.name in routes:
            return p.name, routes[p.name]
    return None, None

def issue(lst, line, code, field=None, detail=None):
    lst.append({"line":line,"code":code,"field":field,"detail":detail})

def validate_file(path, route):
    issues, rows = [], 0
    if not (route.get("table") or "").strip():
        issue(issues, 0, "MISSING_REQUIRED_FIELD", "table", "cannot derive table from source folder")
    try:
        with open(path, encoding="utf-8", errors="strict", newline="") as f:
            for line_no, raw in enumerate(f, 1):
                raw = raw.rstrip("\r\n")
                if raw == "":
                    issue(issues, line_no, "EMPTY_ROW", detail="physical line is empty")
                    continue

                rows += 1
                parts = raw.split(";", 9)   # keep semicolons inside XMLRECORD
                if len(parts) != EXPECTED_PHYSICAL_FIELDS:
                    issue(
                        issues, line_no, "BAD_PHYSICAL_LAYOUT",
                        detail=f"expected 10 fields, found {len(parts)}"
                    )
                    continue

                values = {name: clean(parts[i]) for name, i in IDX.items()}

                for field in REQUIRED_ROW_FIELDS:
                    if values[field] == "":
                        issue(issues, line_no, "MISSING_REQUIRED_FIELD", field, "empty/null")

                op = values["optype"].upper()
                if op and op not in VALID_OPS:
                    issue(issues, line_no, "INVALID_OPTYPE", "optype",
                          f"unsupported value {values['optype']!r}; expected I/U/D/P")

                # XMLRECORD is intentionally NOT parsed.
                # xid/opseqno are allowed to be empty.
    except UnicodeDecodeError as e:
        issue(issues, 0, "INVALID_UTF8", detail=str(e))
    except OSError as e:
        issue(issues, 0, "FILE_READ_ERROR", detail=str(e))

    if rows == 0:
        issue(issues, 0, "NO_CHANGE_ROWS", detail="file contains no non-empty change rows")
    return rows, issues

def write_log(path, bad):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as out:
        out.write(f"T24 corruption report {datetime.now(timezone.utc).isoformat()}\n")
        out.write("="*100 + "\n\n")
        for x in bad:
            out.write(f"CORRUPT file={x['file']}\n")
            out.write(f"  folder={x.get('folder') or 'UNKNOWN'}\n")
            out.write(f"  topic={x.get('topic') or 'UNKNOWN'}\n")
            out.write(f"  derived_table={x.get('table') or 'UNKNOWN'}\n")
            out.write(f"  rows_seen={x['rows']}\n")
            for i in x["issues"]:
                s = [f"line={i['line']}", f"code={i['code']}"]
                if i.get("field"): s.append(f"field={i['field']}")
                if i.get("detail"): s.append(f"detail={i['detail']}")
                out.write("  " + " ".join(s) + "\n")
            out.write("\n")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-root", default="/data/T24")
    p.add_argument("--definitions", default="topic_definitions.json")
    p.add_argument("--log-dir", default="logs")
    p.add_argument("--batch")
    a = p.parse_args()

    root = Path(a.input_root).resolve()
    defs = Path(a.definitions).resolve()
    scan = root / a.batch if a.batch else root

    if not scan.is_dir():
        print(f"ERROR: scan root not found: {scan}", file=sys.stderr); return 2
    if not defs.is_file():
        print(f"ERROR: definitions not found: {defs}", file=sys.stderr); return 2

    routes = load_routes(defs)
    bad, total_rows, total_files, valid_files = [], 0, 0, 0

    for path in sorted(scan.rglob("*.txt")):
        total_files += 1
        folder, route = find_route(path, routes)
        if route is None:
            rows = 0
            issues = [{"line":0,"code":"UNKNOWN_SOURCE_FOLDER","field":"table",
                       "detail":"file is not under a configured source folder"}]
        else:
            rows, issues = validate_file(path, route)
        total_rows += rows
        if issues:
            bad.append({
                "file":str(path), "folder":folder,
                "topic":route.get("topic") if route else None,
                "table":route.get("table") if route else None,
                "rows":rows, "issues":issues
            })
        else:
            valid_files += 1

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log = Path(a.log_dir).resolve() / f"corrupt_files_{a.batch or 'all'}_{stamp}.log"
    if bad:
        write_log(log, bad)

    print("============================================================")
    print("T24 VALIDATION SUMMARY")
    print("============================================================")
    print(f"Files scanned   : {total_files}")
    print(f"Valid files     : {valid_files}")
    print(f"Corrupt files   : {len(bad)}")
    print(f"Change rows seen: {total_rows}")

    if bad:
        print(f"Corruption log  : {log}")
        print("VALIDATION FAILED")
        return 1

    print("VALIDATION PASSED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
