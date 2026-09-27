#!/usr/bin/env bash
# Flink SQL client, with the Fluss and Iceberg catalogs already registered.
#   ./scripts/flink-sql.sh                                          interactive session
#   ./scripts/flink-sql.sh "SET 'execution.runtime-mode' = 'batch';
#                           SELECT count(*) FROM downloads\$lake;"   statements, then exit
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ]; then
  exec docker compose run --rm sql-client
fi

docker compose run --rm -T sql-client bash -c "cat > /tmp/q.sql <<'EOF'
$1
EOF
bash /opt/sql-client.sh -f /tmp/q.sql" 2>/dev/null | grep -E '^[+|]|ERROR|\[INFO\] (Execute|Submitting|SQL update)'
