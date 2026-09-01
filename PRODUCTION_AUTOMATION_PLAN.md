# Production Automation Plan

This repository started as evidence for one BDS-to-AIDP same-tenancy validation
run. The reusable automation layer should be treated as a starter framework, not
as production-ready migration software until it has been validated against
multiple databases, table formats, partition layouts, and network/IAM setups.

## Current Validation Result

Validated path:

```text
BDS Hive/HDFS -> Object Storage -> AIDP external tables -> AIDP SQL validation
```

The validated run used sample Hive external CSV tables on BDS `nams_test`, then
registered matching AIDP external tables in catalog `default`, schema
`migration_lab_sample_20260818`.

The reusable smoke test on 2026-08-27 UTC validated the upgraded path against a
fresh Object Storage prefix and AIDP schema:

```text
BDS HDFS -> BDS Hadoop connector hdfs-oci -> Object Storage
-> AIDP Spark SQL external tables -> AIDP SQL validation
```

Target schema:

```text
default.bds_aidp_live_reusable_export_20260827
```

## Feedback Addressed

The original scripts were environment-specific:

- cluster names, principals, HDFS paths, bucket names, and AIDP resources were
  hard-coded
- temporary Bastion access was manual
- file transfer included local operator staging
- AIDP table registration succeeded through Spark SQL in a notebook, while the
  AIDP create-table API path returned an SQL grammar error

The reusable layer addresses the first-order automation gap by separating:

- Hive inventory
- HDFS-to-Object Storage export
- AIDP external table registration
- row-count validation
- environment configuration
- temporary Bastion access planning and cleanup artifacts
- AIDP create-table API payload generation for product/API debugging

The 2026-08-27 smoke test further validates that migration data can move
directly from BDS HDFS to Object Storage using `--transfer-mode hdfs-oci`. Local
operator transfers are now limited to scripts, manifests, and evidence.

## Registration Strategy

The current primary registration method is **Spark SQL DDL inside an AIDP
workflow**. That is the only method validated successfully in this run.

The AIDP create-table API path is treated as a product/API investigation item:

- `scripts/aidp_generate_create_table_requests.py` generates request bodies from
  the same Object Storage manifest used by the Spark SQL registration path.
- Those payloads are intended to reproduce the API grammar error and validate
  the correct external-table API contract once AIDP engineering confirms it.
- Production migration should not silently fall back from API to Spark SQL. It
  should record the selected registration method in the run evidence.

The AIDP Spark SQL registration script now emits:

```text
registration_method: SPARK_SQL_DDL
api_create_table_status: NOT_USED_CREATE_TABLE_API_FAILED_IN_VALIDATED_RUN
```

## Access and Transfer Strategy

The original validated run used a temporary Bastion and manual file transfers.
The upgraded automation direction is:

- prefer an existing private route to the BDS utility or edge node
- if temporary Bastion is required, generate reviewed create/session/cleanup
  artifacts with `scripts/oci_bastion_access_plan.py`
- run inventory and export from the BDS node
- copy from BDS HDFS directly to Object Storage with
  `scripts/bds_export_to_object_storage.py --transfer-mode hdfs-oci`
- avoid laptop-mediated data transfer for future migration runs

This does not claim Bastion lifecycle is fully productized. It converts an
untracked manual step into a generated, reviewable, repeatable access plan.

The 2026-08-27 smoke test used the approved original BDS SSH private key and did
not require a new Bastion. If that key or a private route is unavailable, the
operator still needs OCI permissions for Bastion, managed SSH, or instance-agent
remote command execution.

When using BDS Object Storage API-key auth, the automation must create a
temporary BDS API key, test the Object Storage connection, pass the
`BDS_OSS_CLIENT_*` connector environment plus matching `HADOOP_OPTS` system
properties to `hdfs dfs`, and delete the key after export. The smoke test
confirmed create/test/delete work requests all succeeded.

## Proposed Workflow

1. Create a run-specific configuration from
   `config/migration_config.example.env`.
2. If private BDS access is not available, generate and review a Bastion access
   plan:

   ```bash
   scripts/oci_bastion_access_plan.py \
     --run-id "<run_id>" \
     --compartment-id "<compartment_ocid>" \
     --target-subnet-id "<subnet_ocid>" \
     --client-cidr "<operator_ip>/32" \
     --target-private-ip "<bds_utility_private_ip>" \
     --ssh-public-key-file "~/.ssh/id_rsa.pub"
   ```

3. On the BDS utility or edge node, inventory source Hive databases:

   ```bash
   scripts/bds_hive_inventory.py \
     --database "<hive_db>" \
     --count-rows \
     --output-dir "runs/<run_id>/inventory"
   ```

4. On the BDS utility or edge node, export HDFS-backed table files and upload
   them directly to Object Storage:

   ```bash
scripts/bds_export_to_object_storage.py \
  --inventory "runs/<run_id>/inventory/hive_inventory.json" \
  --namespace "<namespace>" \
  --bucket "<bucket>" \
  --prefix "migrations/<run_id>" \
  --transfer-mode hdfs-oci \
  --overwrite \
  --output-dir "runs/<run_id>/export"
```

5. Upload `runs/<run_id>/export/object_storage_manifest.json` and
   `scripts/aidp_register_external_tables.py` to AIDP.
6. Run the AIDP registration script in an AIDP notebook/workflow:

   ```bash
python aidp_register_external_tables.py \
  --manifest object_storage_manifest.json \
  --catalog default \
  --schema "migration_<run_id>" \
  --create-mode if-not-exists
```

Use a new Object Storage prefix for each independent target schema. AIDP
rejects duplicate external locations across tables/volumes. Reruns against the
same migrated prefix should be idempotent against the same target schema/table
names, not duplicated into a second schema.

7. Generate AIDP create-table API payloads for the AIDP API issue:

   ```bash
   scripts/aidp_generate_create_table_requests.py \
     --manifest "runs/<run_id>/export/object_storage_manifest.json" \
     --catalog-key default \
     --schema "migration_<run_id>" \
     --output-dir "runs/<run_id>/aidp-api-requests"
   ```

8. Review the emitted registration JSON, generated API payloads, and workflow
   logs.

## Production Gaps

These gaps should be closed before positioning this as production migration
automation:

- automate Bastion/session creation and cleanup or replace it with a standard
  private access pattern. The current script generates the plan and cleanup
  scripts, but live lifecycle execution still needs environment validation.
- support partitioned tables and preserve partition metadata
- add format-specific handling for Parquet, ORC, Avro, compressed text, and
  custom SerDes
- add scalable transfer mode such as DistCp for large tables
- add retry/idempotency controls for OCI Object Storage uploads
- add table allow/deny lists and migration batching
- add richer validation beyond counts: checksums, null checks, min/max checks,
  and customer-defined SQL validations
- resolve the AIDP create-table API SQL grammar failure or confirm the supported
  API request shape for external CSV tables. Generated payloads now make this
  reproducible.
- track the AIDP cluster start CLI behavior where an empty `{}` body is rejected
  as null, while a non-empty body such as `{"properties":{}}` starts the compute

## Engineering Positioning

The correct product statement is:

> We have validated the BDS-source migration pattern through Object Storage and
> AIDP external tables. The current reusable scripts are a starter automation
> kit. They are not yet production-hardened migration software.
