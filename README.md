# BDS to AIDP Same-Tenancy Migration Scripts

Date prepared: 2026-08-19

This folder contains the scripts and request bodies used for the `nams_test` BDS-to-AIDP same-tenancy migration validation.

No passwords or private keys are included.

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

## Execution Order Used

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

## Notes for Engineering

- These scripts are validation scripts, not production migration automation.
- The shell scripts assume they run on the BDS utility node with access to BDS service keytabs and `sudo -u hdfs` / `sudo -u hive`.
- The AIDP Python script assumes an AIDP Spark notebook runtime where `spark` is available.
- Temporary network access and OCI Bastion lifecycle were performed outside these scripts.
- The final evidence report is in:

```text
/Users/namritashukla/Documents/Personal_test_chatGpt/qa-runs/bds_to_aidp_hive_sample_full_migration_20260818/report.md
```
