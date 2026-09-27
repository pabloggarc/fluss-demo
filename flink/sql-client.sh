#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/sql
for f in /opt/sql/*.sql; do
  envsubst '${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${POSTGRES_USER} ${POSTGRES_PASSWORD}' \
    < "$f" > "/tmp/sql/$(basename "$f")"
done

exec /opt/flink/bin/sql-client.sh -i /tmp/sql/init.sql "$@"
