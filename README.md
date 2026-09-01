# BDS to AIDP Same-Tenancy Migration Scripts

Date prepared: 2026-08-19

This folder contains the scripts and request bodies used for the `nams_test`
BDS-to-AIDP same-tenancy migration validation and the reusable smoke test.

No passwords or private keys are included.

## Repository Status

This repository now contains two layers:

1. **Validated run evidence**: the original hard-coded scripts used for the `nams_test` sample migration.
2. **Reusable automation starter kit**: parameter-driven scripts under `scripts/` plus example configuration under `config/`.

The validated path is:

```text
BDS Hive/HDFS -> Object Storage -> AIDP external tables -> AIDP SQL validation
```

The reusable layer is not yet production-hardened migration software. It is the
starting point for production automation and still needs validation across real
customer table formats, partitioning patterns, IAM models, and data volumes.

## Latest Reusable Smoke Test

On 2026-08-27 UTC, the reusable path was validated end to end:

```text
BDS HDFS on nams_test
-> BDS Hadoop connector hdfs-oci copy
-> Object Storage prefix raw/bds_aidp_live_reusable_export_20260827
-> AIDP Spark SQL external tables
-> AIDP validation workflow
```

No migration data was staged on the operator laptop. Local copies were limited
to scripts, manifests, and evidence artifacts.

The target AIDP tables were created as external Spark SQL tables in:

```text
catalog: default
schema: bds_aidp_live_reusable_export_20260827
```

## Scope

- Tenancy: `lakecustomer1`
- BDS cluster: `nams_test`
- BDS region: `us-ashburn-1`
- BDS utility node private IP: `10.0.0.156`
- AIDP region: `uk-london-1`
- AIDP workspace: `test_nams`
- AIDP compute: `tesr_nams`
- Object Storage namespace: `idlhizlfs5zd`
- Object Storage bucket: `migration-nams-same-tenancy-data`

## Files

| File | Runs where | Purpose |
| --- | --- | --- |
| `create_sample_hive_objects.sh` | BDS utility node | Creates the controlled Hive database, writes CSV files into HDFS, and registers Hive external tables. |
| `verify_sample_hive_objects.sh` | BDS utility node | Verifies Hive metadata, bounded Hive reads, HDFS file layout, HDFS row counts, and source sales aggregates. |
| `export_hive_sample_from_hdfs.sh` | BDS utility node | Exports the three HDFS-backed Hive CSV files from HDFS into local utility-node staging for SCP/download. |
| `register_and_validate_aidp_tables.py` | AIDP notebook/workflow | Creates AIDP external tables over Object Storage and validates row counts, sales aggregates, and region join behavior. |
| `aidp-requests/*.json` | Local OCI/AIDP CLI context | Request bodies used for AIDP schema creation, AIDP create-table attempts, and AIDP compute start. |
| `config/migration_config.example.env` | Operator shell / automation runner | Example parameter file for source, target, Object Storage, and runtime settings. |
| `scripts/bds_hive_inventory.py` | BDS utility or edge node | Inventories one or more Hive databases and emits `hive_inventory.json`. |
| `scripts/bds_export_to_object_storage.py` | BDS utility or edge node | Reads Hive inventory, exports HDFS files, uploads to Object Storage, and emits `object_storage_manifest.json`. |
| `scripts/aidp_register_external_tables.py` | AIDP notebook/workflow | Reads the Object Storage manifest, registers external Spark SQL tables, and validates row counts. |
| `scripts/aidp_generate_create_table_requests.py` | Local or AIDP workspace context | Generates AIDP create-table API request bodies from the manifest for API contract debugging. |
| `scripts/oci_bastion_access_plan.py` | Operator shell / CI runner | Generates temporary Bastion create/session/cleanup artifacts when private BDS access is not available. |
| `docs/APPROACH_TEST_PLAN.md` | Engineering/product review | Defines how to test the migration approach separately from simple resource health checks. |
| `docs/PRODUCTION_AUTOMATION_PLAN.md` | Engineering/product review | Describes production gaps, positioning, and next hardening work. |

## Reusable Automation Workflow

Use this flow for the next automation iteration. Run the BDS-side steps from a
BDS node that has Hive/HDFS access. Prefer `BDS_EXPORT_TRANSFER_MODE=hdfs-oci`
with a temporary BDS Object Storage API key, so the copy is performed by the BDS
Hadoop connector and not by local operator staging.

0. If private access to the BDS node is not already available, generate a temporary Bastion plan:

```bash
scripts/oci_bastion_access_plan.py \
  --run-id "${MIGRATION_RUN_ID}" \
  --compartment-id "${BASTION_COMPARTMENT_ID}" \
  --target-subnet-id "${BASTION_TARGET_SUBNET_ID}" \
  --client-cidr "${BASTION_CLIENT_CIDR}" \
  --target-private-ip "${BDS_TARGET_PRIVATE_IP}" \
  --ssh-public-key-file "${BASTION_SSH_PUBLIC_KEY_FILE}"
```

The generated `create_bastion_access.sh` and `cleanup_bastion_access.sh` scripts must be reviewed before execution. If `BASTION_CLIENT_CIDR` is `0.0.0.0/0`, the generator requires `--allow-wide-cidr` so broad temporary access is an explicit approval decision.

1. Create a run config:

```bash
cp config/migration_config.example.env .env
```

2. Source the config after filling in real values:

```bash
set -a
. ./.env
set +a
```

3. Inventory source Hive tables:

```bash
scripts/bds_hive_inventory.py \
  --database "<hive_database>" \
  --count-rows \
  --output-dir "runs/${MIGRATION_RUN_ID}/inventory"
```

4. Export HDFS-backed files directly from the BDS node to Object Storage:

```bash
scripts/bds_export_to_object_storage.py \
  --inventory "runs/${MIGRATION_RUN_ID}/inventory/hive_inventory.json" \
  --namespace "${OBJECT_STORAGE_NAMESPACE}" \
  --bucket "${OBJECT_STORAGE_BUCKET}" \
  --prefix "${OBJECT_STORAGE_PREFIX}" \
  --transfer-mode "${BDS_EXPORT_TRANSFER_MODE}" \
  --overwrite \
  --output-dir "runs/${MIGRATION_RUN_ID}/export"
```

5. Upload these two files to an AIDP workspace/notebook folder:

```text
scripts/aidp_register_external_tables.py
runs/${MIGRATION_RUN_ID}/export/object_storage_manifest.json
```

6. Run the AIDP registration script in an AIDP Spark notebook/workflow:

```bash
python aidp_register_external_tables.py \
  --manifest object_storage_manifest.json \
  --catalog "${AIDP_TARGET_CATALOG}" \
  --schema "${AIDP_TARGET_SCHEMA}" \
  --create-mode if-not-exists
```

Expected output is a JSON summary with target table names, Object Storage locations, and row-count validation status.

AIDP does not allow two tables or volumes to share the same external Object
Storage location. For repeat tests, either reuse the original target schema with
`--create-mode if-not-exists`, or export to a fresh Object Storage prefix before
registering a separate target schema.

7. Generate AIDP create-table API payloads for product/API debugging:

```bash
scripts/aidp_generate_create_table_requests.py \
  --manifest "runs/${MIGRATION_RUN_ID}/export/object_storage_manifest.json" \
  --catalog-key "${AIDP_TARGET_CATALOG}" \
  --schema "${AIDP_TARGET_SCHEMA}" \
  --output-dir "runs/${MIGRATION_RUN_ID}/aidp-api-requests"
```

These payloads are not the default registration path. They preserve a reproducible API input for the AIDP create-table grammar issue while Spark SQL remains the validated registration method.

## Execution Order Used

### 2026-08-27 reusable smoke test

1. Used the approved `mykey` SSH key to access the `nams_test` BDS utility node.
2. Created a temporary BDS Object Storage API key with alias
   `bdsAidpMig20260827b`.
3. Ran `oci bds bds-api-key test-bds-object-storage-connection` against:

```text
oci://migration-nams-same-tenancy-data@idlhizlfs5zd/
```

4. Exported the three Hive-backed HDFS CSV datasets directly from BDS to:

```text
oci://migration-nams-same-tenancy-data@idlhizlfs5zd/raw/bds_aidp_live_reusable_export_20260827/
```

5. Registered AIDP external tables with Spark SQL DDL in catalog `default`,
   schema `bds_aidp_live_reusable_export_20260827`.
6. Validated AIDP counts and sales aggregates against source HDFS values.
7. Stopped AIDP compute `tesr_nams`.
8. Deleted the temporary BDS Object Storage API key and removed local/remote
   temporary credential files.

### Original sample migration run

1. Confirm BDS cluster `nams_test` is active.
2. Establish temporary access to the BDS utility node.
   - OCI Compute Run Command was tried first and failed with `NotAuthorizedOrNotFound`.
   - A temporary OCI Bastion and SSH port-forwarding session were used after approval.
3. Run `create_sample_hive_objects.sh` on the BDS utility node.
4. Run `verify_sample_hive_objects.sh` on the BDS utility node.
5. Run `export_hive_sample_from_hdfs.sh` on the BDS utility node.
6. Copy the exported files from the utility node to local staging.
7. Upload the exported files to Object Storage:

```text
oci://migration-nams-same-tenancy-data@idlhizlfs5zd/raw/migration_lab_sample_20260818/hive_export_20260818/
```

8. Create or confirm AIDP schema:

```text
default.migration_lab_sample_20260818
```

9. Start AIDP compute `tesr_nams` if needed.
10. Run `register_and_validate_aidp_tables.py` in an AIDP notebook/workflow.
11. Confirm AIDP workflow status is `SUCCESS`.
12. Confirm AIDP tables are `ACTIVE` and `EXTERNAL`.
13. Delete temporary Bastion session and Bastion.

## Source Hive Objects Created

Hive database:

```text
migration_lab_sample_20260818
```

Hive tables:

```text
migration_lab_sample_20260818.customers_csv
migration_lab_sample_20260818.sales_csv
migration_lab_sample_20260818.regions_csv
```

HDFS source base:

```text
/warehouse/tablespace/external/hive/migration_lab_sample_20260818.db/raw
```

## AIDP Tables Created

Reusable smoke-test target:

```text
default.bds_aidp_live_reusable_export_20260827.customers_csv
default.bds_aidp_live_reusable_export_20260827.sales_csv
default.bds_aidp_live_reusable_export_20260827.regions_csv
```

Original target:

```text
default.migration_lab_sample_20260818.customers_csv
default.migration_lab_sample_20260818.sales_csv
default.migration_lab_sample_20260818.regions_csv
```

## Validation Values

Row counts:

| Table | Expected and validated |
| --- | ---: |
| `customers_csv` | 6 |
| `sales_csv` | 8 |
| `regions_csv` | 5 |

Sales aggregate:

| Region | Orders | Total Amount |
| --- | ---: | ---: |
| `apac` | 1 | 45.75 |
| `emea` | 2 | 445.40 |
| `latam` | 1 | 99.99 |
| `us-east` | 2 | 675.60 |
| `us-west` | 2 | 297.25 |

## Known Issue

The AIDP `schema create-table` CLI/API path was attempted with the JSON files under `aidp-requests/`, but failed with:

```text
InvalidParameter: The SQL command is incorrect. Check the grammar.
```

The successful table-registration path was the AIDP notebook script `register_and_validate_aidp_tables.py`, using Spark SQL DDL.

The reusable package now makes that decision explicit:

- `scripts/aidp_register_external_tables.py` is the primary validated registration path and emits `registration_method: SPARK_SQL_DDL`.
- `scripts/aidp_generate_create_table_requests.py` generates API payloads for AIDP engineering/product to reproduce and debug the create-table API grammar failure.

## Notes for Engineering

- These scripts are validation scripts, not production migration automation.
- The shell scripts assume they run on the BDS utility node with access to BDS service keytabs and `sudo -u hdfs` / `sudo -u hive`.
- The AIDP Python script assumes an AIDP Spark notebook runtime where `spark` is available.
- Temporary network access and OCI Bastion lifecycle were manual in the original
  validated run. The reusable smoke test used the approved BDS SSH key instead
  of creating a Bastion. The package includes
  `scripts/oci_bastion_access_plan.py` to generate reviewed
  create/session/cleanup artifacts when Bastion access is needed.
- The Bastion generator was smoke-tested against the current OCI CLI schema on
  2026-08-23. Current CLI JSON uses `clientCidrList`, `maxSessionTtl`,
  `sshPublicKeyFile`, `sessionTtl`, `targetPrivateIp`, and `targetPort`.
- BDS source-side live execution still requires either the original cluster SSH
  private key, a preapproved private route, or OCI permissions that allow
  managed SSH / instance-agent command execution on the BDS utility node.
- The original validated run copied files through local staging. The 2026-08-27
  reusable smoke test validated direct BDS-node export through
  `--transfer-mode hdfs-oci`, removing laptop-mediated migration data transfer.
- The reusable scripts under `scripts/` parameterize the hard-coded values but still need production hardening for large data volumes, partitioned tables, non-CSV formats, retries, and fully automated access/workflow orchestration.
- The final evidence report is in:

```text
/Users/namritashukla/Documents/Personal_test_chatGpt/qa-runs/bds_to_aidp_hive_sample_full_migration_20260818/report.md
```
