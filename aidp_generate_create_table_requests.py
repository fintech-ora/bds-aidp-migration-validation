#!/usr/bin/env python3
"""Generate AIDP create-table API request bodies from an export manifest.

This does not make the AIDP API the production path. The current validated
registration path is Spark SQL in an AIDP workflow. These generated payloads
make the API failure reproducible so AIDP engineering can confirm the supported
contract or fix the SQL grammar issue.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    return cleaned.strip("._-") or "table"


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


def api_type(hive_type: str) -> tuple[str, str, str]:
    value = (hive_type or "string").strip().lower()
    if value.startswith("decimal"):
        match = re.search(r"\((\d+)\s*,\s*(\d+)\)", value)
        if match:
            return "decimal", match.group(1), match.group(2)
        return "decimal", "38", "10"
    if value in {"tinyint", "smallint", "int", "integer"}:
        return "int", "10", "0"
    if value == "bigint":
        return "bigint", "19", "0"
    if value in {"float", "double"}:
        return value, "30", "15"
    if value == "boolean":
        return "boolean", "1", "0"
    if value in {"date", "timestamp"}:
        return value, "0", "0"
    return "string", "4000", "0"


def table_fields(table: dict[str, Any]) -> list[dict[str, str]]:
    fields = []
    for column in table.get("columns") or []:
        field_type, precision, scale = api_type(column.get("type", "string"))
        fields.append(
            {
                "fieldName": column["name"],
                "fieldType": field_type,
                "fieldPrecision": precision,
                "fieldScale": scale,
            }
        )
    return fields


def data_format(table: dict[str, Any]) -> str:
    value = (table.get("data_format") or "csv").upper()
    if value not in {"CSV", "PARQUET", "ORC", "AVRO"}:
        return "CSV"
    return value


def external_definition(table: dict[str, Any]) -> dict[str, Any]:
    definition: dict[str, Any] = {
        "externalTableLocationType": "OBJECT_STORAGE",
        "objectStorageLocationPath": table["object_storage_location"],
        "externalTableDataFormat": data_format(table),
    }
    if definition["externalTableDataFormat"] == "CSV":
        csv = table.get("csv") or {}
        definition["txtFileDefinition"] = {
            "delimiter": str(csv.get("delimiter", ",")),
            "quote": "\\\"",
        }
    return definition


def table_properties(table: dict[str, Any], source_label: str) -> list[dict[str, str]]:
    csv = table.get("csv") or {}
    data_fmt = data_format(table)
    props = [
        {"propertyName": "migration.source", "propertyValue": source_label},
        {"propertyName": "lakehouse_storage_format", "propertyValue": data_fmt},
    ]
    if data_fmt == "CSV":
        delimiter = str(csv.get("delimiter", ","))
        header = str(csv.get("header", "true")).lower()
        props.extend(
            [
                {"propertyName": "option.header", "propertyValue": header},
                {"propertyName": "header", "propertyValue": header},
                {"propertyName": "option.delimiter", "propertyValue": delimiter},
                {"propertyName": "field.delim", "propertyValue": delimiter},
                {"propertyName": "fileformat", "propertyValue": "TEXTFILE"},
            ]
        )
    return props


def request_body(table: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    source_table = table.get("qualified_name") or f"{table.get('database')}.{table.get('table_name')}"
    source_label = f"bds:hive:{source_table}"
    schema_key = args.schema_key or f"{args.catalog_key}.{args.schema}"
    return {
        "catalogKey": args.catalog_key,
        "schemaKey": schema_key,
        "displayName": table["table_name"],
        "description": f"External table migrated from BDS Hive table {source_table}.",
        "tableType": "EXTERNAL",
        "externalTableDefinition": external_definition(table),
        "tableFields": table_fields(table),
        "partitionKeys": [],
        "tableProperties": table_properties(table, source_label),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", help="Path to object_storage_manifest.json.")
    parser.add_argument("--manifest-json", help="Inline manifest JSON.")
    parser.add_argument("--catalog-key", default=os.environ.get("AIDP_TARGET_CATALOG", "default"))
    parser.add_argument("--schema", default=os.environ.get("AIDP_TARGET_SCHEMA"))
    parser.add_argument("--schema-key", default=os.environ.get("AIDP_TARGET_SCHEMA_KEY"))
    parser.add_argument("--output-dir", default=os.environ.get("AIDP_REQUEST_OUTPUT_DIR", "runs/aidp-api-requests"))
    args = parser.parse_args()
    if not args.schema and not args.schema_key:
        raise SystemExit("--schema, --schema-key, AIDP_TARGET_SCHEMA, or AIDP_TARGET_SCHEMA_KEY is required")
    return args


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = []
    for table in manifest.get("tables", []):
        if table.get("status") == "SKIPPED":
            continue
        body = request_body(table, args)
        path = output_dir / f"create-table-{safe_name(table['table_name'])}.json"
        path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs.append(
            {
                "table": table["table_name"],
                "request_body": str(path),
                "object_storage_location": table.get("object_storage_location"),
            }
        )

    summary = {
        "status": "AIDP_CREATE_TABLE_API_PAYLOADS_GENERATED",
        "note": (
            "Payloads are for API contract debugging. Use Spark SQL registration "
            "until the create-table API grammar issue is resolved."
        ),
        "catalog_key": args.catalog_key,
        "schema_key": args.schema_key or f"{args.catalog_key}.{args.schema}",
        "outputs": outputs,
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
