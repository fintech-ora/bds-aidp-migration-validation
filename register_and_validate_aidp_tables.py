"""
AIDP notebook code used for the BDS-to-AIDP same-tenancy Hive sample migration.

Run context:
- Intended to run inside an AIDP notebook/workflow attached to compute `tesr_nams`.
- Creates external tables in `default.migration_lab_sample_20260818`.
- Validates row counts and sales aggregates against the BDS/HDFS source values.

No credentials are embedded in this script.
"""

import json
from decimal import Decimal


SCHEMA = "default.migration_lab_sample_20260818"
SOURCE_DB = "migration_lab_sample_20260818"
OBJECT_BASE = (
    "oci://migration-nams-same-tenancy-data@idlhizlfs5zd/"
    "raw/migration_lab_sample_20260818/hive_export_20260818"
)

TABLES = {
    "customers_csv": {
        "location": f"{OBJECT_BASE}/customers_csv",
        "columns": "customer_id STRING, customer_name STRING, segment STRING, home_region STRING",
        "expected_count": 6,
    },
    "sales_csv": {
        "location": f"{OBJECT_BASE}/sales_csv",
        "columns": "order_id INT, customer_id STRING, region STRING, amount DOUBLE, order_date STRING",
        "expected_count": 8,
    },
    "regions_csv": {
        "location": f"{OBJECT_BASE}/regions_csv",
        "columns": "region STRING, region_name STRING, quota DOUBLE",
        "expected_count": 5,
    },
}

EXPECTED_TOTALS = [
    {"region": "apac", "order_count": 1, "total_amount": "45.75"},
    {"region": "emea", "order_count": 2, "total_amount": "445.40"},
    {"region": "latam", "order_count": 1, "total_amount": "99.99"},
    {"region": "us-east", "order_count": 2, "total_amount": "675.60"},
    {"region": "us-west", "order_count": 2, "total_amount": "297.25"},
]

create_results = []


def create_table(table_name, spec):
    fqtn = f"{SCHEMA}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {fqtn}")

    hive_sql = f"""
    CREATE TABLE {fqtn} ({spec['columns']})
    USING hive
    OPTIONS ('delimiter'=',','field.delim'=',','fileformat'='TEXTFILE')
    LOCATION '{spec['location']}'
    TBLPROPERTIES (
      'field.delim'=',',
      'header'='true',
      'option.header'='true',
      'lakehouse_storage_format'='CSV',
      'fileformat'='TEXTFILE'
    )
    """

    try:
        spark.sql(hive_sql)
        create_results.append(
            {"table": fqtn, "method": "USING hive", "status": "SUCCESS"}
        )
        return fqtn
    except Exception as hive_exc:
        csv_sql = f"""
        CREATE TABLE {fqtn} ({spec['columns']})
        USING csv
        OPTIONS (path '{spec['location']}', header 'true', delimiter ',')
        """
        spark.sql(csv_sql)
        create_results.append(
            {
                "table": fqtn,
                "method": "USING csv",
                "status": "SUCCESS_AFTER_HIVE_DDL_FAILURE",
                "hive_error": str(hive_exc)[:1000],
            }
        )
        return fqtn


for table_name, spec in TABLES.items():
    create_table(table_name, spec)

registered = [row.tableName for row in spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()]

counts = {}
for table_name, spec in TABLES.items():
    fqtn = f"{SCHEMA}.{table_name}"
    row_count = int(
        spark.sql(f"SELECT COUNT(*) AS row_count FROM {fqtn}").collect()[0][
            "row_count"
        ]
    )
    counts[table_name] = row_count
    if row_count != spec["expected_count"]:
        raise RuntimeError(
            f"Unexpected row count for {fqtn}: expected {spec['expected_count']}, got {row_count}"
        )

total_rows = spark.sql(
    f"""
SELECT region, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS total_amount
FROM {SCHEMA}.sales_csv
GROUP BY region
ORDER BY region
"""
).collect()

actual_totals = []
for row in total_rows:
    actual_totals.append(
        {
            "region": row["region"],
            "order_count": int(row["order_count"]),
            "total_amount": format(Decimal(str(row["total_amount"])), ".2f"),
        }
    )

if actual_totals != EXPECTED_TOTALS:
    raise RuntimeError(
        json.dumps({"expected": EXPECTED_TOTALS, "actual": actual_totals}, sort_keys=True)
    )

join_rows = spark.sql(
    f"""
SELECT s.region, COUNT(*) AS order_count, ROUND(SUM(s.amount), 2) AS total_amount, MAX(r.quota) AS quota
FROM {SCHEMA}.sales_csv s
LEFT JOIN {SCHEMA}.regions_csv r ON s.region = r.region
GROUP BY s.region
ORDER BY s.region
"""
).collect()

join_result = [
    {
        "region": row["region"],
        "order_count": int(row["order_count"]),
        "total_amount": format(Decimal(str(row["total_amount"])), ".2f"),
        "quota": format(Decimal(str(row["quota"])), ".2f"),
    }
    for row in join_rows
]

result = {
    "status": "AIDP_HIVE_SAMPLE_MIGRATION_VALIDATION_SUCCESS",
    "source_hive_database": SOURCE_DB,
    "target_schema": SCHEMA,
    "object_storage_base": OBJECT_BASE,
    "create_results": create_results,
    "registered_tables": sorted([t for t in registered if t in TABLES]),
    "row_counts": counts,
    "region_totals": actual_totals,
    "region_totals_with_quota": join_result,
}

payload = json.dumps(result, sort_keys=True)
print(payload)

try:
    dbutils.notebook.exit(payload)
except NameError:
    pass
