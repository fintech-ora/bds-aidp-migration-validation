# Approach Test Plan

This test plan validates the migration approach, not only whether BDS and AIDP
resources are active.

## Scope

Validate the reusable approach introduced after the original environment-specific
run:

```text
BDS Hive inventory
-> BDS-node HDFS export
-> direct BDS-node Object Storage upload
-> Object Storage manifest
-> AIDP Spark SQL external-table registration
-> validation evidence
```

## Approach Assertions

The approach is considered valid only if these assertions pass:

1. BDS cluster metadata can identify the utility node, private IP, and subnet
   needed for controlled access.
2. Temporary Bastion access is represented as generated create/session/cleanup
   artifacts, not as an undocumented manual step.
3. The HDFS-to-Object Storage export path runs from the BDS node, avoiding
   laptop-mediated data transfer.
4. AIDP table registration records Spark SQL DDL as the selected validated
   method.
5. AIDP create-table API payloads are generated separately for product/API
   debugging and are not treated as the default successful registration path.
6. AIDP workflow creation can be generated for the Spark SQL registration path.
7. Live validation compares target row counts to source counts collected from
   Hive inventory or direct HDFS file reads.

## No-Write Test

This stage is safe to run without creating infrastructure or tables:

1. Resolve BDS `nams_test` metadata.
2. Resolve AIDP workspace/catalog/compute metadata.
3. Generate Bastion access and cleanup artifacts from real BDS metadata.
4. Generate AIDP create-table API request bodies from the real migrated table
   definitions and Object Storage locations.
5. Run Spark SQL registration in dry-run mode and confirm the evidence fields:

   ```text
   registration_method: SPARK_SQL_DDL
   api_create_table_status: NOT_USED_CREATE_TABLE_API_FAILED_IN_VALIDATED_RUN
   ```

6. Dry-run AIDP workspace upload and workflow creation.

## Live Smoke Test

This stage changes live resources and requires explicit approval:

1. Create temporary Bastion/session or use an approved private route.
2. Copy or invoke the reusable scripts on the BDS utility node.
3. Run Hive inventory for a controlled source database/table set.
4. Export from HDFS directly from the BDS node to a new Object Storage test
   prefix, preferably with the BDS Hadoop connector `hdfs-oci` transfer mode.
5. Generate the Object Storage manifest and API debug payloads.
6. Start AIDP compute if needed.
7. Register AIDP external tables in a new test schema with Spark SQL DDL.
8. Validate row counts against the source inventory.
9. Collect workflow/log evidence.
10. Clean up Bastion/session. Optionally clean up test schema and Object
    Storage prefix after evidence is captured.

Use a fresh Object Storage prefix for every new target schema. AIDP rejects
external table creation when another table or volume already uses the same
location. For reruns against an already migrated prefix, use the same target
schema/table names with idempotent `CREATE TABLE IF NOT EXISTS` semantics.

## Pass/Fail Criteria

The approach passes only when:

- the live run avoids local laptop data staging
- all migrated table locations point to the generated Object Storage prefix
- AIDP registration evidence states `SPARK_SQL_DDL`
- generated API payloads exist for the same tables
- row counts match source counts
- temporary access cleanup is confirmed

## 2026-08-27 Live Reusable Smoke Notes

- Used approved direct SSH access with `/Users/namritashukla/Desktop/mykey`;
  no new Bastion was required.
- Created temporary BDS Object Storage API key alias
  `bdsAidpMig20260827b` after adding PMDomain OCID to the create request.
- BDS Object Storage connection test succeeded for:

  ```text
  oci://migration-nams-same-tenancy-data@idlhizlfs5zd/
  ```

- BDS Hadoop connector probe succeeded from `nams_test` as `hdfs`.
- `scripts/bds_export_to_object_storage.py --transfer-mode hdfs-oci` exported
  three HDFS-backed Hive CSV tables directly from BDS to:

  ```text
  raw/bds_aidp_live_reusable_export_20260827/
  ```

- Object Storage verification found non-zero CSV objects for `customers_csv`,
  `regions_csv`, and `sales_csv`.
- AIDP compute `tesr_nams` was started for validation. The CLI accepted
  `cluster start` only when the body was non-empty (`{"properties":{}}`);
  an empty `{}` body was serialized as null and rejected by AIDP.
- AIDP Spark SQL DDL registered external tables in:

  ```text
  default.bds_aidp_live_reusable_export_20260827
  ```

- AIDP validation workflow succeeded. Source HDFS counts were collected with
  direct HDFS file reads because Hive `COUNT(*)` was slow in the earlier
  inventory run.
- Temporary BDS API key deletion succeeded, AIDP compute was stopped, and local
  plus remote temp credential files were removed.

## 2026-08-23 Live Smoke Notes

- Temporary Bastion creation succeeded after the generator was updated to the
  current OCI CLI JSON field names.
- Port-forwarding to the BDS utility node was established, but SSH login failed
  because the local key was not authorized on the BDS host.
- OCI managed SSH and instance-agent command checks were blocked by
  `NotAuthorizedOrNotFound`, so live BDS-side inventory/export could not be run
  from this environment.
- AIDP compute `tesr_nams` was started successfully.
- AIDP registration against a second schema over the already migrated Object
  Storage locations failed with the expected duplicate-location conflict.
- AIDP idempotent registration/validation against
  `default.migration_lab_sample_20260818` succeeded.
