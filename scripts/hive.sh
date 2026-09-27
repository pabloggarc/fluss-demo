#!/usr/bin/env bash
# Hive CLI against the metastore.
#   ./scripts/hive.sh                                        interactive session
#   ./scripts/hive.sh "DESCRIBE FORMATTED bronze.downloads"  single statement
# Derby is embedded and locked by the running metastore, so the CLI goes through its thrift API.
set -euo pipefail
cd "$(dirname "$0")/.."

HIVE=(hive --hiveconf hive.metastore.uris=thrift://localhost:9083 --hiveconf hive.execution.engine=mr)

if [ $# -eq 0 ]; then
  exec docker compose exec metastore "${HIVE[@]}"
fi
docker compose exec -T metastore "${HIVE[@]}" -S -e "$1" 2>/dev/null
