#!/usr/bin/env bash
set -euo pipefail

DB="migration_lab_sample_20260818"
HDFS_BASE="/warehouse/tablespace/external/hive/${DB}.db"

HDFS_KEYTAB="/etc/security/keytabs/hdfs.headless.keytab"
HDFS_PRINCIPAL="hdfs-namstest@BDSCLOUDSERVICE.ORACLE.COM"
HDFS_CCACHE="/tmp/krb5cc_hdfs_${DB}_verify"

HIVE_KEYTAB="/etc/security/keytabs/hive.service.keytab"
HIVE_PRINCIPAL="hive/$(hostname -f)@BDSCLOUDSERVICE.ORACLE.COM"
HIVE_CCACHE="/tmp/krb5cc_hive_${DB}_verify"

echo "=== Verification context ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
hostname -f
echo "Hive database: ${DB}"
echo "External data base: ${HDFS_BASE}"

sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" kinit -kt "${HIVE_KEYTAB}" "${HIVE_PRINCIPAL}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" kinit -kt "${HDFS_KEYTAB}" "${HDFS_PRINCIPAL}"

echo "=== Hive objects ==="
timeout 60 sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -e "SHOW DATABASES LIKE '${DB}'; USE ${DB}; SHOW TABLES;"

echo "=== Hive fast table reads ==="
timeout 60 sudo -u hive env KRB5CCNAME="${HIVE_CCACHE}" hive -S -e "SET hive.fetch.task.conversion=more; USE ${DB}; SELECT * FROM customers_csv LIMIT 3; SELECT * FROM sales_csv LIMIT 3; SELECT * FROM regions_csv LIMIT 3;" || echo "Hive LIMIT read did not complete within timeout"

echo "=== HDFS file layout ==="
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -ls -R "${HDFS_BASE}"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -du -h "${HDFS_BASE}/raw"

echo "=== HDFS row counts ==="
printf "customers_csv\t"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -cat "${HDFS_BASE}/raw/customers/customers.csv" | awk -F, 'NR > 1 {count++} END {print count + 0}'
printf "sales_csv\t"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -cat "${HDFS_BASE}/raw/sales/sales.csv" | awk -F, 'NR > 1 {count++} END {print count + 0}'
printf "regions_csv\t"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -cat "${HDFS_BASE}/raw/regions/regions.csv" | awk -F, 'NR > 1 {count++} END {print count + 0}'

echo "=== HDFS sales aggregate by region ==="
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -cat "${HDFS_BASE}/raw/sales/sales.csv" \
  | awk -F, 'NR > 1 {count[$3]++; sum[$3] += $4} END {for (region in count) printf "%s\t%d\t%.2f\n", region, count[region], sum[region]}' \
  | sort

echo "=== Complete ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
