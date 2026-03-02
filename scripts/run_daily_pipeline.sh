#!/usr/bin/env bash
set -euo pipefail

DT="${1:-}"
if [[ -z "$DT" ]]; then
  echo "Usage: ./scripts/run_daily_pipeline.sh YYYY-MM-DD"
  exit 1
fi

VEHICLES="${VEHICLES:-5}"
MINUTES="${MINUTES:-30}"

# Required env var
: "${BUCKET:?Set BUCKET env var, e.g. export BUCKET=fleet-datalake-...}"

echo "=== Fleet Telemetry Daily Pipeline ==="
echo "Date: $DT | Vehicles: $VEHICLES | Minutes: $MINUTES"
echo "Bucket: $BUCKET"
echo

echo "[1/4] Generate telemetry (Bronze/local_raw)"
python scripts/generate_telemetry.py --date "$DT" --vehicles "$VEHICLES" --minutes "$MINUTES"

echo "[2/4] Upload Bronze to S3"
aws s3 cp "data/local_raw/dt=$DT" "s3://$BUCKET/bronze/telemetry/dt=$DT/" --recursive

echo "[3/4] Spark ETL → Parquet (Silver/local_processed)"
python spark_jobs/json_to_parquet.py --date "$DT"

echo "[4/4] Upload Silver to S3"
aws s3 cp "data/local_processed/telemetry/dt=$DT" \
  "s3://$BUCKET/silver/telemetry/dt=$DT/" \
  --recursive --exclude "*.crc"

echo
echo "✅ Done."
echo "Bronze: s3://$BUCKET/bronze/telemetry/dt=$DT/"
echo "Silver : s3://$BUCKET/silver/telemetry/dt=$DT/"
