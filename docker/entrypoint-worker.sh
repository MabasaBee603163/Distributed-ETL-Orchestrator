#!/usr/bin/env bash
set -euo pipefail

# Defaults (override via environment)
: "${PREFECT_WORK_POOL:=etl-pool}"
: "${PREFECT_WORKER_TYPE:=process}"
: "${PREFECT_WORK_QUEUES:=}" # space-separated list -> multiple -q flags
: "${PREFECT_WORKER_NAME:=}"
: "${PREFECT_WORKER_CONCURRENCY_LIMIT:=}"

if [[ -z "${PREFECT_API_URL:-}" ]]; then
  echo "ERROR: PREFECT_API_URL is required for workers (Prefect API / Cloud)." >&2
  exit 1
fi

args=(prefect worker start --pool "${PREFECT_WORK_POOL}" --type "${PREFECT_WORKER_TYPE}")

if [[ -n "${PREFECT_WORKER_NAME:-}" ]]; then
  args+=(--name "${PREFECT_WORKER_NAME}")
fi

if [[ -n "${PREFECT_WORKER_CONCURRENCY_LIMIT:-}" ]]; then
  args+=(--limit "${PREFECT_WORKER_CONCURRENCY_LIMIT}")
fi

if [[ -n "${PREFECT_WORK_QUEUES:-}" ]]; then
  # shellcheck disable=SC2206
  queues=(${PREFECT_WORK_QUEUES})
  for q in "${queues[@]}"; do
    args+=(--work-queue "${q}")
  done
fi

exec "${args[@]}"
