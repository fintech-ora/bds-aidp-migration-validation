#!/usr/bin/env python3
"""Inventory Hive databases/tables on a BDS utility node.

The script is intentionally parameter-driven so it can replace the one-off
hard-coded sample scripts. It writes a JSON manifest that downstream export and
AIDP registration steps can consume.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def run_command(cmd: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, check=False)


def wrap_as_user(cmd: list[str], user: str | None, krb5ccname: str | None) -> list[str]:
    env_parts = []
    if krb5ccname:
        env_parts.append(f"KRB5CCNAME={krb5ccname}")
    if user:
        wrapped = ["sudo", "-u", user]
        if env_parts:
            wrapped.extend(["env", *env_parts])
        wrapped.extend(cmd)
        return wrapped
    if env_parts:
        return ["env", *env_parts, *cmd]
    return cmd


def maybe_kinit(args: argparse.Namespace) -> None:
    if not args.hive_keytab or not args.hive_principal:
        return
    cmd = wrap_as_user(
        ["kinit", "-kt", args.hive_keytab, args.hive_principal],
        args.hive_user,
        args.hive_krb5ccname,
    )
    result = run_command(cmd, args.timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Hive kinit failed\n"
            f"command: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def hive_query(sql: str, args: argparse.Namespace) -> str:
    cmd = wrap_as_user(
        [args.hive_bin, "-S", "-e", sql],
        args.hive_user,
        args.hive_krb5ccname,
    )
    result = run_command(cmd, args.timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Hive query failed\n"
            f"sql: {sql}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result.stdout


def clean_output_lines(output: str) -> list[str]:
    lines = []
    for line in output.splitlines():
        value = line.strip()
        if not value:
            continue
        if value.startswith("WARN ") or value.startswith("SLF4J:"):
            continue
        lines.append(value)
    return lines


def parse_tables(output: str) -> list[str]:
    tables = []
    for line in clean_output_lines(output):
        if line.lower() in {"tab_name", "database_name"}:
            continue
        if line.startswith("#"):
            continue
        tables.append(line.split()[0])
    return sorted(set(tables))


def parse_columns(output: str) -> list[dict[str, str]]:
    columns: list[dict[str, str]] = []
    for raw in output.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if stripped.startswith(("WARN ", "WARN:", "SLF4J:")):
            continue
        if not line.strip():
            if columns:
                break
            continue
        if line.lstrip().startswith("#"):
            continue
        parts = [part.strip() for part in line.split("\t")]
        if len(parts) < 2:
            parts = re.split(r"\s+", line.strip(), maxsplit=2)
        if len(parts) < 2:
            continue
        name, data_type = parts[0], parts[1]
        if not name or not data_type:
            continue
        if name.lower() in {"col_name", "partition information"}:
            continue
        if name.startswith("#"):
            continue
        columns.append({"name": name, "type": data_type})
    return columns


def parse_formatted_metadata(output: str) -> dict[str, str]:
    wanted = {
        "Database",
        "Owner",
        "CreateTime",
        "LastAccessTime",
        "Retention",
        "Location",
        "Table Type",
        "InputFormat",
        "OutputFormat",
        "Serde Library",
        "Num Buckets",
    }
    metadata: dict[str, str] = {}
    for raw in output.splitlines():
        parts = [part.strip() for part in raw.split("\t") if part.strip()]
        if len(parts) < 2:
            continue
        key = parts[0].rstrip(":")
        if key in wanted:
            metadata[key] = parts[1]
    return metadata


def parse_row_count(output: str) -> int:
    for line in reversed(clean_output_lines(output)):
        match = re.search(r"(-?\d+)\s*$", line)
        if match:
            return int(match.group(1))
    raise ValueError(f"Unable to parse row count from Hive output: {output!r}")


def selected_tables_for_db(database: str, args: argparse.Namespace) -> list[str]:
    explicit = []
    for value in args.table:
        if "." in value:
            db, table = value.split(".", 1)
            if db == database:
                explicit.append(table)
        elif len(args.database) == 1:
            explicit.append(value)
    if explicit:
        return sorted(set(explicit))

    output = hive_query(f"USE {qident(database)}; SHOW TABLES;", args)
    tables = parse_tables(output)
    if args.table_regex:
        pattern = re.compile(args.table_regex)
        tables = [table for table in tables if pattern.search(table)]
    return tables


def inventory_table(database: str, table: str, args: argparse.Namespace) -> dict[str, Any]:
    fqtn = f"{qident(database)}.{qident(table)}"
    describe_output = hive_query(f"DESCRIBE {fqtn};", args)
    formatted_output = hive_query(f"DESCRIBE FORMATTED {fqtn};", args)
    metadata = parse_formatted_metadata(formatted_output)

    item: dict[str, Any] = {
        "database": database,
        "table_name": table,
        "qualified_name": f"{database}.{table}",
        "table_type": metadata.get("Table Type"),
        "location": metadata.get("Location"),
        "input_format": metadata.get("InputFormat"),
        "output_format": metadata.get("OutputFormat"),
        "serde_library": metadata.get("Serde Library"),
        "columns": parse_columns(describe_output),
    }

    if args.count_rows:
        try:
            count_output = hive_query(f"SELECT COUNT(*) FROM {fqtn};", args)
            item["row_count"] = parse_row_count(count_output)
        except Exception as exc:  # Keep inventory useful when one count is slow/broken.
            item["row_count_error"] = str(exc)

    return item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", action="append", required=True, help="Hive database to inventory. Repeat for multiple databases.")
    parser.add_argument("--table", action="append", default=[], help="Optional table or db.table filter. Repeat for multiple tables.")
    parser.add_argument("--table-regex", help="Optional regex filter applied when --table is not supplied.")
    parser.add_argument("--include-views", action="store_true", help="Include Hive views in the output.")
    parser.add_argument("--count-rows", action="store_true", help="Run SELECT COUNT(*) for each table. This can be slow on large sources.")
    parser.add_argument("--output-dir", default=os.environ.get("MIGRATION_OUTPUT_DIR", "runs/hive-inventory"))
    parser.add_argument("--hive-bin", default=os.environ.get("HIVE_BIN", "hive"))
    parser.add_argument("--hive-user", default=os.environ.get("HIVE_USER", "hive"))
    parser.add_argument("--hive-keytab", default=os.environ.get("HIVE_KEYTAB"))
    parser.add_argument("--hive-principal", default=os.environ.get("HIVE_PRINCIPAL"))
    parser.add_argument("--hive-krb5ccname", default=os.environ.get("HIVE_KRB5CCNAME"))
    parser.add_argument("--timeout-seconds", type=int, default=int(os.environ.get("HIVE_TIMEOUT_SECONDS", "300")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    maybe_kinit(args)

    tables: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for database in args.database:
        for table in selected_tables_for_db(database, args):
            try:
                item = inventory_table(database, table, args)
                if item.get("table_type") == "VIRTUAL_VIEW" and not args.include_views:
                    continue
                tables.append(item)
            except Exception as exc:
                errors.append({"database": database, "table": table, "error": str(exc)})

    payload = {
        "generated_at_utc": utc_now(),
        "tool": "bds_hive_inventory.py",
        "source": {
            "type": "BDS_HIVE",
            "databases": args.database,
        },
        "tables": tables,
        "errors": errors,
    }

    output_path = output_dir / "hive_inventory.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    if errors:
        print(json.dumps({"errors": errors}, indent=2), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
