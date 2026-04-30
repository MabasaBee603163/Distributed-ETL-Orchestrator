#!/usr/bin/env bash
set -euo pipefail

# Download CSV from S3 (optional) then run the CSV→Supabase flow.
# Intended for ECS scheduled tasks.

: "${CSV_PATH:=/tmp/input.csv}"

if [[ -n "${CSV_S3_BUCKET:-}" && -n "${CSV_S3_KEY:-}" ]]; then
  aws s3 cp "s3://${CSV_S3_BUCKET}/${CSV_S3_KEY}" "${CSV_PATH}"
fi

exec python main.py run-csv --csv "${CSV_PATH}"

