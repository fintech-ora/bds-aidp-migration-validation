#!/usr/bin/env python3
"""Generate a reproducible OCI Bastion access and cleanup plan.

The validated migration used temporary Bastion access manually. This script does
not hide that dependency; it turns it into explicit generated artifacts that can
be reviewed, approved, executed, and cleaned up consistently.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any


PLACEHOLDER = "<required>"


def env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def utc_now_compact() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def oci_prefix(args: argparse.Namespace) -> str:
    parts = [args.oci_bin]
    if args.oci_profile:
        parts.extend(["--profile", shell_quote(args.oci_profile)])
    if args.oci_region:
        parts.extend(["--region", shell_quote(args.oci_region)])
    if args.oci_auth:
        parts.extend(["--auth", shell_quote(args.oci_auth)])
    return " ".join(parts)


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def read_public_key(path: str) -> str:
    expanded = Path(path).expanduser()
    if not expanded.exists():
        return f"<public-key-content-from-{path}>"
    return expanded.read_text(encoding="utf-8").strip()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def bastion_create_body(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "bastionType": "STANDARD",
        "clientCidrList": [args.client_cidr],
        "compartmentId": args.compartment_id,
        "maxSessionTtl": args.max_session_ttl_seconds,
        "name": args.bastion_name,
        "targetSubnetId": args.target_subnet_id,
    }


def session_create_body(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "bastionId": "<bastion-ocid-from-create-response>",
        "displayName": args.session_name,
        "keyType": "PUB",
        "sessionTtl": str(args.session_ttl_seconds),
        "sshPublicKeyFile": str(Path(args.ssh_public_key_file).expanduser()),
        "targetPort": str(args.target_port),
        "targetPrivateIp": args.target_private_ip,
    }


def command_script(args: argparse.Namespace, output_dir: Path) -> str:
    prefix = oci_prefix(args)
    bastion_json = output_dir / "bastion-create.json"
    session_json = output_dir / "session-create-port-forwarding.json"
    return f"""#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"

echo "Create temporary Bastion"
{prefix} bastion bastion create --from-json "file://${{RUN_DIR}}/{bastion_json.name}" > "${{RUN_DIR}}/bastion-create-response.json"

BASTION_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["id"])' "${{RUN_DIR}}/bastion-create-response.json")"
python3 - "$BASTION_ID" "${{RUN_DIR}}/{session_json.name}" <<'PY'
import json
import sys
body = json.load(open(sys.argv[2]))
body["bastionId"] = sys.argv[1]
json.dump(body, open(sys.argv[2], "w"), indent=2, sort_keys=True)
print()
PY

echo "Create port-forwarding session"
{prefix} bastion session create-port-forwarding --from-json "file://${{RUN_DIR}}/{session_json.name}" > "${{RUN_DIR}}/session-create-response.json"

SESSION_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["id"])' "${{RUN_DIR}}/session-create-response.json")"
echo "$BASTION_ID" > "${{RUN_DIR}}/bastion.id"
echo "$SESSION_ID" > "${{RUN_DIR}}/session.id"

echo
echo "Review the SSH metadata from the session response and start the tunnel."
echo "OCI usually returns the exact SSH command in data.ssh-metadata.command."
python3 - "$RUN_DIR/session-create-response.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1]))["data"]
print(json.dumps(data.get("ssh-metadata", {{}}), indent=2, sort_keys=True))
PY

echo
echo "Expected local forwarding target:"
echo "  127.0.0.1:{args.local_port} -> {args.target_private_ip}:{args.target_port}"
"""


def cleanup_script(args: argparse.Namespace, output_dir: Path) -> str:
    prefix = oci_prefix(args)
    return f"""#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"

if [ -f "${{RUN_DIR}}/session.id" ]; then
  SESSION_ID="$(cat "${{RUN_DIR}}/session.id")"
  echo "Delete Bastion session $SESSION_ID"
  {prefix} bastion session delete --session-id "$SESSION_ID" --force || true
fi

if [ -f "${{RUN_DIR}}/bastion.id" ]; then
  BASTION_ID="$(cat "${{RUN_DIR}}/bastion.id")"
  echo "Delete Bastion $BASTION_ID"
  {prefix} bastion bastion delete --bastion-id "$BASTION_ID" --force || true
fi
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=env("MIGRATION_RUN_ID", f"bds-aidp-{utc_now_compact()}"))
    parser.add_argument("--output-dir", default=env("BASTION_PLAN_DIR"))
    parser.add_argument("--compartment-id", default=env("BASTION_COMPARTMENT_ID", PLACEHOLDER))
    parser.add_argument("--target-subnet-id", default=env("BASTION_TARGET_SUBNET_ID", PLACEHOLDER))
    parser.add_argument("--client-cidr", default=env("BASTION_CLIENT_CIDR", PLACEHOLDER))
    parser.add_argument("--target-private-ip", default=env("BDS_TARGET_PRIVATE_IP", PLACEHOLDER))
    parser.add_argument("--target-port", type=int, default=int(env("BDS_TARGET_SSH_PORT", "22")))
    parser.add_argument("--local-port", type=int, default=int(env("LOCAL_SSH_FORWARD_PORT", "22022")))
    parser.add_argument("--ssh-public-key-file", default=env("BASTION_SSH_PUBLIC_KEY_FILE", "~/.ssh/id_rsa.pub"))
    parser.add_argument("--bastion-name", default=env("BASTION_NAME"))
    parser.add_argument("--session-name", default=env("BASTION_SESSION_NAME"))
    parser.add_argument("--max-session-ttl-seconds", type=int, default=int(env("BASTION_MAX_SESSION_TTL_SECONDS", "10800")))
    parser.add_argument("--session-ttl-seconds", type=int, default=int(env("BASTION_SESSION_TTL_SECONDS", "10800")))
    parser.add_argument("--oci-bin", default=env("OCI_CLI_BIN", "oci"))
    parser.add_argument("--oci-profile", default=env("OCI_PROFILE"))
    parser.add_argument("--oci-region", default=env("OCI_REGION"))
    parser.add_argument("--oci-auth", default=env("OCI_AUTH"))
    parser.add_argument("--allow-wide-cidr", action="store_true", help="Allow 0.0.0.0/0 in generated Bastion plan.")
    args = parser.parse_args()
    if args.client_cidr == "0.0.0.0/0" and not args.allow_wide_cidr:
        raise SystemExit("Refusing to generate 0.0.0.0/0 Bastion plan without --allow-wide-cidr.")
    if not args.bastion_name:
        args.bastion_name = f"bds-aidp-{args.run_id}"
    if not args.session_name:
        args.session_name = f"bds-aidp-{args.run_id}-pf"
    if not args.output_dir:
        args.output_dir = f"runs/{args.run_id}/bastion"
    return args


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_json(output_dir / "bastion-create.json", bastion_create_body(args))
    write_json(output_dir / "session-create-port-forwarding.json", session_create_body(args))

    access_path = output_dir / "create_bastion_access.sh"
    cleanup_path = output_dir / "cleanup_bastion_access.sh"
    access_path.write_text(command_script(args, output_dir), encoding="utf-8")
    cleanup_path.write_text(cleanup_script(args, output_dir), encoding="utf-8")
    access_path.chmod(0o755)
    cleanup_path.chmod(0o755)

    summary = {
        "status": "BASTION_ACCESS_PLAN_GENERATED",
        "note": "Review and approve generated commands before execution. Prefer private access over temporary Bastion when available.",
        "create_script": str(access_path),
        "cleanup_script": str(cleanup_path),
        "client_cidr": args.client_cidr,
        "target": f"{args.target_private_ip}:{args.target_port}",
    }
    summary_path = output_dir / "summary.json"
    write_json(summary_path, summary)
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
