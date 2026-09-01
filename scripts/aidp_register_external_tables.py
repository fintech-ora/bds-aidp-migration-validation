#!/usr/bin/env python3
"""Register exported Object Storage data as AIDP external Spark SQL tables.

Run this inside an AIDP notebook/workflow attached to a Spark compute. It reads
the object_storage_manifest.json produced by bds_export_to_object_storage.py.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any


def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fqtn(catalog: str, schema: str, table: str) -> str:
    return ".".join(qident(part) for part in (catalog, schema, table))


def normalize_type(hive_type: str) -> str:
    value = hive_type.strip().lower()
    if value.startswith("varchar") or value.startswith("char"):
        return "STRING"
    if value.startswith("decimal"):
        return value.upper()
    mapping = {
        "string": "STRING",
        "tinyint": "TINYINT",
        "smallint": "SMALLINT",
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "binary": "BINARY",
    }
    return mapping.get(value, "STRING")


def safe_table_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not cleaned:
        raise ValueError(f"Invalid empty table name from {name!r}")
    if cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned


def table_columns_sql(table: dict[str, Any]) -> str:
    columns = table.get("columns") or []
    if not columns:
        raise ValueError(f"No columns found for {table.get('qualified_name') or table.get('table_name')}")
    rendered = []
    for column in columns:
        rendered.append(f"{qident(column['name'])} {normalize_type(column['type'])}")
    return ",\n  ".join(rendered)


def create_table_sql(catalog: str, schema: str, table: dict[str, Any], create_mode: str) -> list[str]:
    table_name = safe_table_name(table["table_name"])
    target = fqtn(catalog, schema, table_name)
    location = table["object_storage_location"]
    data_format = (table.get("data_format") or "csv").lower()
    columns_sql = table_columns_sql(table)
    statements = []
    if create_mode == "replace":
        statements.append(f"DROP TABLE IF EXISTS {target}")
    create_clause = "CREATE TABLE"
    if create_mode == "if-not-exists":
        create_clause = "CREATE TABLE IF NOT EXISTS"

    if data_format == "csv":
        csv = table.get("csv") or {}
        header = str(csv.get("header", "true")).lower()
        delimiter = str(csv.get("delimiter", ",")).replace("'", "\\'")
        statements.append(
            f"""{create_clause} {target} (
  {columns_sql}
)
USING csv
OPTIONS (header '{header}', delimiter '{delimiter}')
LOCATION '{location}'"""
        )
    elif data_format in {"parquet", "orc", "avro"}:
        statements.append(
            f"""{create_clause} {target} (
  {columns_sql}
)
USING {data_format}
LOCATION '{location}'"""
        )
    else:
        raise ValueError(f"Unsupported data format for {table_name}: {data_format}")
    return statements


def load_manifest(args: argparse.Namespace) -> dict[str, Any]:
    if args.manifest_json:
        return json.loads(args.manifest_json)
    if args.manifest:
        return json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    env_json = os.environ.get("MIGRATION_MANIFEST_JSON")
    if env_json:
        return json.loads(env_json)
    env_path = os.environ.get("MIGRATION_MANIFEST_PATH")
    if env_path:
        return json.loads(Path(env_path).read_text(encoding="utf-8"))
    raise ValueError("Provide --manifest, --manifest-json, MIGRATION_MANIFEST_PATH, or MIGRATION_MANIFEST_JSON.")


def execute_sql(statement: str, dry_run: bool) -> Any:
    if dry_run:
        print(statement.rstrip() + ";\n")
        return None
    if "spark" not in globals():
        raise RuntimeError("Spark session named 'spark' is required unless --dry-run is used.")
    return globals()["spark"].sql(statement)


def collect_count(table_name: str, dry_run: bool) -> int | None:
    if dry_run:
        return None
    rows = execute_sql(f"SELECT COUNT(*) AS row_count FROM {table_name}", dry_run).collect()
    return int(rows[0]["row_count"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", help="Path to object_storage_manifest.json.")
    parser.add_argument("--manifest-json", help="Inline manifest JSON.")
    parser.add_argument("--catalog", default=os.environ.get("AIDP_TARGET_CATALOG", "default"))
    parser.add_argument("--schema", default=os.environ.get("AIDP_TARGET_SCHEMA"))
    parser.add_argument("--create-mode", choices=["replace", "if-not-exists"], default=os.environ.get("AIDP_CREATE_MODE", "if-not-exists"))
    parser.add_argument("--registration-method", choices=["spark-sql"], default=os.environ.get("AIDP_REGISTRATION_METHOD", "spark-sql"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    args, _unknown = parser.parse_known_args()
    if not args.schema:
        raise SystemExit("--schema or AIDP_TARGET_SCHEMA is required")
    return args


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args)
    catalog = args.catalog
    schema = args.schema
    results = []

    execute_sql(f"CREATE SCHEMA IF NOT EXISTS {qident(catalog)}.{qident(schema)}", args.dry_run)

    for table in manifest.get("tables", []):
        if table.get("status") == "SKIPPED":
            results.append({"table": table.get("table_name"), "status": "SKIPPED", "reason": table.get("reason")})
            continue
        for statement in create_table_sql(catalog, schema, table, args.create_mode):
            try:
                execute_sql(statement, args.dry_run)
            except Exception as exc:
                message = str(exc).lower()
                if "conflict with the location" in message:
                    raise RuntimeError(
                        "AIDP rejected the external table because another table or volume already uses "
                        f"the same location: {table.get('object_storage_location')}. Re-run against the "
                        "existing target table with --create-mode if-not-exists, or export to a new "
                        "Object Storage prefix before registering a separate test table."
                    ) from exc
                raise

        target = fqtn(catalog, schema, safe_table_name(table["table_name"]))
        actual_count = None
        expected_count = table.get("source_row_count")
        if not args.skip_validation:
            actual_count = collect_count(target, args.dry_run)
            if expected_count is not None and actual_count is not None and int(expected_count) != actual_count:
                raise RuntimeError(
                    f"Row-count mismatch for {target}: source={expected_count}, target={actual_count}"
                )
        results.append(
            {
                "source_table": table.get("qualified_name"),
                "target_table": target,
                "object_storage_location": table.get("object_storage_location"),
                "expected_count": expected_count,
                "actual_count": actual_count,
                "status": "PLANNED" if args.dry_run else "REGISTERED",
            }
        )

    payload = {
        "status": "DRY_RUN" if args.dry_run else "AIDP_EXTERNAL_TABLE_REGISTRATION_COMPLETE",
        "registration_method": "SPARK_SQL_DDL",
        "api_create_table_status": "NOT_USED_CREATE_TABLE_API_FAILED_IN_VALIDATED_RUN",
        "target_catalog": catalog,
        "target_schema": schema,
        "create_mode": args.create_mode,
        "tables": results,
    }
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    print(text)
    try:
        dbutils.notebook.exit(text)  # type: ignore[name-defined]
    except NameError:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
