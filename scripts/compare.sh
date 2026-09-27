#!/usr/bin/env bash
# Completed downloads per tool and time of the last one, in Postgres, in the Fluss union
# read and in Iceberg only. The union read runs in streaming mode because batch union read
# of primary-key tables is not supported for Iceberg in Fluss 1.0
set -euo pipefail
cd "$(dirname "$0")/.."

SECS=${1:-25}

query() {
  echo "SELECT tool, count(*) AS downloads, max(created_at) AS last_download
FROM $1
WHERE status = 'completed'
GROUP BY tool"
}

flink_sql() {
  docker compose run --rm -T sql-client bash -c "cat > /tmp/q.sql <<'EOF'
$1
EOF
${2:-} bash /opt/sql-client.sh -f /tmp/q.sql" 2>/dev/null || true
}

# Every source is turned into "tool|downloads|HH:MM:SS.mmm" lines.
normalize() {
  awk -F'|' '{
    gsub(/^ +| +$/, "", $1); gsub(/^ +| +$/, "", $2); gsub(/^ +| +$/, "", $3)
    split($3, ts, " "); split(ts[2], hms, ".")
    printf "%s|%s|%s.%s\n", $1, $2, hms[1], substr(hms[2] "000", 1, 3)
  }' | sort
}

echo "Running the union read for ${SECS}s..." >&2
union_read=$(flink_sql "SET 'execution.runtime-mode' = 'streaming';
$(query downloads);" "timeout $SECS" \
  | grep -E '^\| (\+I|\+U) ' \
  | awk -F'|' '{ last[$3] = $3 "|" $4 "|" $5 } END { for (t in last) print last[t] }' \
  | normalize)

postgres=$(docker compose exec -T source-db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F"|" -c "$0"' "$(query downloads)" \
  | normalize)

iceberg=$(flink_sql "SET 'execution.runtime-mode' = 'batch';
$(query 'downloads$lake');" \
  | grep -E '^\| +[A-Za-z]' | grep -v ' tool ' \
  | sed -E 's/^\| *//; s/ *\|$//' \
  | normalize)

join -t'|' <(echo "$postgres") <(echo "$union_read") \
  | join -t'|' - <(echo "$iceberg") \
  | awk -F'|' '
    BEGIN {
      printf "%-10s  %-20s  %-20s  %-20s\n", "tool", "Postgres", "Fluss union read", "Iceberg only"
    }
    { printf "%-10s  %5s  %-12s  %5s  %-12s  %5s  %-12s\n", $1, $2, $3, $4, $5, $6, $7 }'
