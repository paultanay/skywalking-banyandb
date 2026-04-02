#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../" && pwd)

cd "$ROOT_DIR"

if [[ -z "${BANYANDB_BENCH_CHART:-}" ]]; then
  echo "BANYANDB_BENCH_CHART not set; using OCI chart ${BANYANDB_BENCH_CHART_VERSION:-0.5.3}."
fi

go test ./test/integration/replication/benchmark -v
