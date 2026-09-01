#!/usr/bin/env bash
set -euo pipefail

DB="migration_lab_sample_20260818"
HDFS_BASE="/warehouse/tablespace/external/hive/${DB}.db/raw"
EXPORT_BASE="/tmp/bds_to_aidp_hive_sample_full_migration_20260818"
HDFS_KEYTAB="/etc/security/keytabs/hdfs.headless.keytab"
HDFS_PRINCIPAL="hdfs-namstest@BDSCLOUDSERVICE.ORACLE.COM"
HDFS_CCACHE="/tmp/krb5cc_hdfs_bds_to_aidp_hive_sample_full_migration_20260818"

echo "=== Export context ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
hostname -f
echo "Hive database: ${DB}"
echo "HDFS raw base: ${HDFS_BASE}"
echo "Local export base: ${EXPORT_BASE}"

sudo rm -rf "${EXPORT_BASE}"
mkdir -p "${EXPORT_BASE}/customers_csv" "${EXPORT_BASE}/sales_csv" "${EXPORT_BASE}/regions_csv"
sudo chown -R hdfs:hdfs "${EXPORT_BASE}"

sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" kinit -kt "${HDFS_KEYTAB}" "${HDFS_PRINCIPAL}"

sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -get -f "${HDFS_BASE}/customers/customers.csv" "${EXPORT_BASE}/customers_csv/customers.csv"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -get -f "${HDFS_BASE}/sales/sales.csv" "${EXPORT_BASE}/sales_csv/sales.csv"
sudo -u hdfs env KRB5CCNAME="${HDFS_CCACHE}" hdfs dfs -get -f "${HDFS_BASE}/regions/regions.csv" "${EXPORT_BASE}/regions_csv/regions.csv"

sudo chown -R opc:opc "${EXPORT_BASE}"
chmod -R u+rwX,go+rX "${EXPORT_BASE}"

echo "=== Exported files ==="
find "${EXPORT_BASE}" -type f -maxdepth 3 -print -exec wc -c {} \; -exec shasum -a 256 {} \;

echo "=== Row counts ==="
printf "customers_csv\t"
awk -F, 'NR > 1 {count++} END {print count + 0}' "${EXPORT_BASE}/customers_csv/customers.csv"
printf "sales_csv\t"
awk -F, 'NR > 1 {count++} END {print count + 0}' "${EXPORT_BASE}/sales_csv/sales.csv"
printf "regions_csv\t"
awk -F, 'NR > 1 {count++} END {print count + 0}' "${EXPORT_BASE}/regions_csv/regions.csv"

echo "=== Sales aggregate by region ==="
awk -F, 'NR > 1 {count[$3]++; sum[$3] += $4} END {for (region in count) printf "%s\t%d\t%.2f\n", region, count[region], sum[region]}' "${EXPORT_BASE}/sales_csv/sales.csv" | sort

echo "=== Complete ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
