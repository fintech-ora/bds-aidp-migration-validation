#!/usr/bin/env bash
set -euo pipefail

DB="migration_lab_sample_20260818"
HDFS_BASE="/warehouse/tablespace/external/hive/${DB}.db"
LEGACY_HDFS_BASE="/migration_lab_sample_20260818"
LOCAL_BASE="/tmp/migration_lab_sample_20260818"

HDFS_KEYTAB="/etc/security/keytabs/hdfs.headless.keytab"
HDFS_PRINCIPAL="hdfs-namstest@BDSCLOUDSERVICE.ORACLE.COM"
HDFS_CCACHE="/tmp/krb5cc_hdfs_${DB}"

HIVE_KEYTAB="/etc/security/keytabs/hive.service.keytab"
HIVE_PRINCIPAL="hive/$(hostname -f)@BDSCLOUDSERVICE.ORACLE.COM"
HIVE_CCACHE="/tmp/krb5cc_hive_${DB}"

echo "=== Context ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
hostname -f
echo "Hive principal: ${HIVE_PRINCIPAL}"
echo "HDFS base: ${HDFS_BASE}"
echo "Hive database: ${DB}"

rm -rf "${LOCAL_BASE}"
mkdir -p "${LOCAL_BASE}/data"
chmod 755 "${LOCAL_BASE}" "${LOCAL_BASE}/data"

cat > "${LOCAL_BASE}/data/customers.csv" <<'CSV'
customer_id,customer_name,segment,home_region
C001,Asha Rao,enterprise,us-east
C002,Ben Miles,commercial,us-west
C003,Clara Mehta,enterprise,emea
C004,Dev Singh,public-sector,us-east
C005,Elena Rossi,commercial,emea
C006,Farah Khan,startup,latam
CSV

cat > "${LOCAL_BASE}/data/sales.csv" <<'CSV'
order_id,customer_id,region,amount,order_date
1001,C001,us-east,125.50,2026-07-01
1002,C002,us-west,87.25,2026-07-01
1003,C003,emea,312.00,2026-07-02
1004,C001,apac,45.75,2026-07-02
1005,C004,us-east,550.10,2026-07-03
1006,C005,emea,133.40,2026-07-03
1007,C002,us-west,210.00,2026-07-04
1008,C006,latam,99.99,2026-07-04
CSV

cat > "${LOCAL_BASE}/data/regions.csv" <<'CSV'
region,region_name,quota
apac,Asia Pacific,50.00
emea,Europe Middle East Africa,400.00
latam,Latin America,75.00
us-east,US East,650.00
us-west,US West,250.00
CSV

chmod 644 "${LOCAL_BASE}/data/"*.csv

cat > "${LOCAL_BASE}/drop_database.sql" <<SQL
DROP DATABASE IF EXISTS ${DB} CASCADE;
SQL

cat > "${LOCAL_BASE}/create_tables.sql" <<SQL
CREATE DATABASE ${DB}
COMMENT 'Sample Hive database for BDS to AIDP migration test';

USE ${DB};

CREATE EXTERNAL TABLE customers_csv (
  customer_id STRING,
  customer_name STRING,
  segment STRING,
  home_region STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw/customers'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'migration.test'='bds-to-aidp-same-tenancy'
);

CREATE EXTERNAL TABLE sales_csv (
  order_id INT,
  customer_id STRING,
  region STRING,
  amount DOUBLE,
  order_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw/sales'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'migration.test'='bds-to-aidp-same-tenancy'
);

CREATE EXTERNAL TABLE regions_csv (
  region STRING,
  region_name STRING,
  quota DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw/regions'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'migration.test'='bds-to-aidp-same-tenancy'
);
SQL

cat > "${LOCAL_BASE}/verify_metadata.sql" <<SQL
SHOW DATABASES LIKE '${DB}';
USE ${DB};
SHOW TABLES;
DESCRIBE FORMATTED customers_csv;
DESCRIBE FORMATTED sales_csv;
DESCRIBE FORMATTED regions_csv;
SQL

cat > "${LOCAL_BASE}/verify_queries.sql" <<SQL
SET hive.cli.print.header=true;
USE ${DB};
SELECT 'customers_csv' AS table_name, COUNT(*) AS row_count FROM customers_csv;
SELECT 'sales_csv' AS table_name, COUNT(*) AS row_count FROM sales_csv;
SELECT 'regions_csv' AS table_name, COUNT(*) AS row_count FROM regions_csv;
SELECT region, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS total_amount
FROM sales_csv
GROUP BY region
ORDER BY region;
SELECT s.region, COUNT(*) AS order_count, ROUND(SUM(s.amount), 2) AS total_amount, MAX(r.quota) AS quota
FROM sales_csv s
LEFT JOIN regions_csv r ON s.region = r.region
GROUP BY s.region
ORDER BY s.region;
SQL

echo "=== Drop previous Hive database if present ==="
sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" kinit -kt "${HIVE_KEYTAB}" "${HIVE_PRINCIPAL}"
sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -f "${LOCAL_BASE}/drop_database.sql"

echo "=== Create HDFS sample data paths ==="
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" kinit -kt "${HDFS_KEYTAB}" "${HDFS_PRINCIPAL}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -rm -r -f "${LEGACY_HDFS_BASE}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -rm -r -f "${HDFS_BASE}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -mkdir -p "${HDFS_BASE}/raw/customers" "${HDFS_BASE}/raw/sales" "${HDFS_BASE}/raw/regions"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -put -f "${LOCAL_BASE}/data/customers.csv" "${HDFS_BASE}/raw/customers/customers.csv"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -put -f "${LOCAL_BASE}/data/sales.csv" "${HDFS_BASE}/raw/sales/sales.csv"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -put -f "${LOCAL_BASE}/data/regions.csv" "${HDFS_BASE}/raw/regions/regions.csv"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -chown -R hive "${HDFS_BASE}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -chmod -R 775 "${HDFS_BASE}"

echo "=== Register Hive database and external tables ==="
sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -f "${LOCAL_BASE}/create_tables.sql"

echo "=== Hive metadata verification ==="
sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -f "${LOCAL_BASE}/verify_metadata.sql"

echo "=== Hive query verification ==="
sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -f "${LOCAL_BASE}/verify_queries.sql"

echo "=== HDFS data verification ==="
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -ls -R "${HDFS_BASE}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -du -h "${HDFS_BASE}/raw"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -cat "${HDFS_BASE}/raw/sales/sales.csv"

echo "=== Complete ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
