#!/usr/bin/env python3
"""Export inventoried BDS Hive/HDFS table files to Object Storage.

This runs on a BDS utility/edge node with HDFS access. The preferred transfer
mode is hdfs-oci, which copies from HDFS to Object Storage through the BDS
Hadoop connector. The oci-cli mode is retained for environments that install
OCI CLI on the BDS node.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def run_command(cmd: list[str], timeout: int, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
    if dry_run:
        print("DRY-RUN:", " ".join(cmd))
        return subprocess.CompletedProcess(cmd, 0, "", "")
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, check=False)


CONNECTOR_ENV_PREFIXES = ("BDS_OSS_CLIENT_", "OCI_SECRET_")
CONNECTOR_ENV_NAMES = ("HADOOP_OPTS",)


def inherited_connector_env() -> list[str]:
    env_parts = []
    for key, value in os.environ.items():
        if key in CONNECTOR_ENV_NAMES or any(key.startswith(prefix) for prefix in CONNECTOR_ENV_PREFIXES):
            env_parts.append(f"{key}={value}")
    return sorted(env_parts)


def wrap_as_user(cmd: list[str], user: str | None, krb5ccname: str | None) -> list[str]:
    env_parts = []
    if krb5ccname:
        env_parts.append(f"KRB5CCNAME={krb5ccname}")
    env_parts.extend(inherited_connector_env())
    if user:
        wrapped = ["sudo", "-u", user]
        if env_parts:
            wrapped.extend(["env", *env_parts])
        wrapped.extend(cmd)
        return wrapped
    if env_parts:
        return ["env", *env_parts, *cmd]
    return cmd


def require_success(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed\nstdout: {result.stdout.strip()}\nstderr: {result.stderr.strip()}")


def prepare_local_staging_dir(path: Path, args: argparse.Namespace) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if not args.hdfs_user:
        return
    result = run_command(["sudo", "-n", "chown", args.hdfs_user, str(path)], args.timeout_seconds, args.dry_run)
    require_success(result, f"Prepare staging owner {path}")


def maybe_kinit(args: argparse.Namespace) -> None:
    if not args.hdfs_keytab or not args.hdfs_principal:
        return
    cmd = wrap_as_user(
        ["kinit", "-kt", args.hdfs_keytab, args.hdfs_principal],
        args.hdfs_user,
        args.hdfs_krb5ccname,
    )
    result = run_command(cmd, args.timeout_seconds, args.dry_run)
    require_success(result, "HDFS kinit")


def load_inventory(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def is_hdfs_location(location: str | None) -> bool:
    if not location:
        return False
    return location.startswith("/") or location.startswith("hdfs://")


def hdfs_cmd(args: argparse.Namespace, *parts: str) -> list[str]:
    return wrap_as_user([args.hdfs_bin, "dfs", *parts], args.hdfs_user, args.hdfs_krb5ccname)


def list_hdfs_files(location: str, args: argparse.Namespace) -> list[str]:
    result = run_command(hdfs_cmd(args, "-ls", "-R", location), args.timeout_seconds, args.dry_run)
    require_success(result, f"HDFS list {location}")
    files = []
    for raw in result.stdout.splitlines():
        line = raw.strip()
        if not line.startswith("-"):
            continue
        files.append(line.rsplit(maxsplit=1)[-1])
    return sorted(files)


def relative_hdfs_path(base: str, path: str) -> str:
    clean_base = base.rstrip("/")
    if path.startswith(clean_base + "/"):
        return path[len(clean_base) + 1 :]
    return Path(path).name


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def infer_data_format(table: dict[str, Any], default_format: str) -> str:
    haystack = " ".join(
        str(table.get(key) or "")
        for key in ("input_format", "output_format", "serde_library")
    ).lower()
    if "parquet" in haystack:
        return "parquet"
    if "orc" in haystack:
        return "orc"
    if "avro" in haystack:
        return "avro"
    return default_format.lower()


def oci_base_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [args.oci_bin]
    if args.oci_profile:
        cmd.extend(["--profile", args.oci_profile])
    if args.oci_region:
        cmd.extend(["--region", args.oci_region])
    if args.oci_auth:
        cmd.extend(["--auth", args.oci_auth])
    return cmd


def upload_file(local_path: Path, object_name: str, args: argparse.Namespace) -> None:
    cmd = [
        *oci_base_cmd(args),
        "os",
        "object",
        "put",
        "--namespace-name",
        args.namespace,
        "--bucket-name",
        args.bucket,
        "--file",
        str(local_path),
        "--name",
        object_name,
    ]
    if args.overwrite:
        cmd.append("--force")
    result = run_command(cmd, args.timeout_seconds, args.dry_run)
    require_success(result, f"OCI upload {object_name}")


def normalized_prefix(*parts: str) -> str:
    return "/".join(str(part).strip("/") for part in parts if str(part).strip("/"))


def export_table(table: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    location = table.get("location")
    if not is_hdfs_location(location):
        return {
            "database": table.get("database"),
            "table_name": table.get("table_name"),
            "status": "SKIPPED",
            "reason": f"Unsupported or missing HDFS location: {location}",
        }

    database = table["database"]
    table_name = table["table_name"]
    table_dir = Path(args.staging_dir) / database / table_name
    prepare_local_staging_dir(table_dir, args)

    hdfs_files = list_hdfs_files(location, args)
    files = []
    object_prefix = normalized_prefix(args.prefix, database, table_name)
    object_storage_location = f"oci://{args.bucket}@{args.namespace}/{object_prefix}"

    if args.transfer_mode == "hdfs-oci":
        result = run_command(
            hdfs_cmd(args, "-mkdir", "-p", object_storage_location),
            args.timeout_seconds,
            args.dry_run,
        )
        require_success(result, f"Object Storage mkdir {object_storage_location}")

    for hdfs_file in hdfs_files:
        rel = relative_hdfs_path(location, hdfs_file)
        object_name = normalized_prefix(object_prefix, rel)
        if args.transfer_mode == "hdfs-oci":
            object_uri = f"oci://{args.bucket}@{args.namespace}/{object_name}"
            if args.overwrite:
                result = run_command(hdfs_cmd(args, "-rm", "-f", object_uri), args.timeout_seconds, args.dry_run)
                if result.returncode != 0 and "No such file" not in result.stderr:
                    require_success(result, f"Remove existing Object Storage object {object_uri}")
            result = run_command(hdfs_cmd(args, "-cp", "-f", hdfs_file, object_uri), args.timeout_seconds, args.dry_run)
            require_success(result, f"HDFS copy to Object Storage {object_uri}")
        else:
            local_file = table_dir / rel
            prepare_local_staging_dir(local_file.parent, args)
            result = run_command(hdfs_cmd(args, "-get", "-f", hdfs_file, str(local_file)), args.timeout_seconds, args.dry_run)
            require_success(result, f"HDFS get {hdfs_file}")

        if not args.skip_upload and args.transfer_mode == "oci-cli":
            object_name = normalized_prefix(object_prefix, rel)
            upload_file(local_file, object_name, args)

        if args.dry_run:
            file_info = {
                "source_hdfs_path": hdfs_file,
                "object_name": object_name,
            }
        elif args.transfer_mode == "hdfs-oci":
            file_info = {
                "source_hdfs_path": hdfs_file,
                "object_name": object_name,
                "transfer_mode": args.transfer_mode,
            }
        else:
            file_info = {
                "source_hdfs_path": hdfs_file,
                "local_staging_path": str(local_file),
                "object_name": object_name,
                "size_bytes": local_file.stat().st_size,
                "sha256": sha256_file(local_file),
            }
        files.append(file_info)

    return {
        "database": database,
        "table_name": table_name,
        "qualified_name": table.get("qualified_name"),
        "source_location": location,
        "table_type": table.get("table_type"),
        "columns": table.get("columns", []),
        "source_row_count": table.get("row_count"),
        "data_format": infer_data_format(table, args.default_format),
        "csv": {
            "header": args.csv_header,
            "delimiter": args.csv_delimiter,
        },
        "object_storage_location": object_storage_location,
        "files": files,
        "status": "EXPORTED" if args.skip_upload else "UPLOADED",
        "transfer_mode": args.transfer_mode,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, help="Path to hive_inventory.json from bds_hive_inventory.py.")
    parser.add_argument("--staging-dir", default=os.environ.get("MIGRATION_STAGING_DIR", "staging/bds-export"))
    parser.add_argument("--output-dir", default=os.environ.get("MIGRATION_OUTPUT_DIR", "runs/bds-export"))
    parser.add_argument("--namespace", default=os.environ.get("OBJECT_STORAGE_NAMESPACE"), required=not bool(os.environ.get("OBJECT_STORAGE_NAMESPACE")))
    parser.add_argument("--bucket", default=os.environ.get("OBJECT_STORAGE_BUCKET"), required=not bool(os.environ.get("OBJECT_STORAGE_BUCKET")))
    parser.add_argument("--prefix", default=os.environ.get("OBJECT_STORAGE_PREFIX", "migrations/default-run"))
    parser.add_argument("--hdfs-bin", default=os.environ.get("HDFS_BIN", "hdfs"))
    parser.add_argument("--hdfs-user", default=os.environ.get("HDFS_USER", "hdfs"))
    parser.add_argument("--hdfs-keytab", default=os.environ.get("HDFS_KEYTAB"))
    parser.add_argument("--hdfs-principal", default=os.environ.get("HDFS_PRINCIPAL"))
    parser.add_argument("--hdfs-krb5ccname", default=os.environ.get("HDFS_KRB5CCNAME"))
    parser.add_argument("--oci-bin", default=os.environ.get("OCI_CLI_BIN", "oci"))
    parser.add_argument("--oci-profile", default=os.environ.get("OCI_PROFILE"))
    parser.add_argument("--oci-region", default=os.environ.get("OCI_REGION"))
    parser.add_argument("--oci-auth", default=os.environ.get("OCI_AUTH"))
    parser.add_argument("--default-format", default="csv", choices=["csv", "parquet", "orc", "avro"])
    parser.add_argument("--csv-header", default=os.environ.get("CSV_HEADER", "true"))
    parser.add_argument("--csv-delimiter", default=os.environ.get("CSV_DELIMITER", ","))
    parser.add_argument("--transfer-mode", choices=["oci-cli", "hdfs-oci"], default=os.environ.get("BDS_EXPORT_TRANSFER_MODE", "oci-cli"))
    parser.add_argument("--skip-upload", action="store_true", help="Only export from HDFS into local staging; do not upload to Object Storage.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite Object Storage objects when they already exist.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=int(os.environ.get("EXPORT_TIMEOUT_SECONDS", "600")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prepare_local_staging_dir(Path(args.staging_dir), args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    maybe_kinit(args)
    inventory = load_inventory(Path(args.inventory))
    exported_tables = [export_table(table, args) for table in inventory.get("tables", [])]

    manifest = {
        "generated_at_utc": utc_now(),
        "tool": "bds_export_to_object_storage.py",
        "source_inventory": str(Path(args.inventory)),
        "object_storage": {
            "namespace": args.namespace,
            "bucket": args.bucket,
            "prefix": args.prefix,
        },
        "tables": exported_tables,
    }

    output_path = output_dir / "object_storage_manifest.json"
    output_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    failures = [table for table in exported_tables if table.get("status") == "SKIPPED"]
    return 2 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
