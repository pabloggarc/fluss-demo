#!/usr/bin/env bash
# Skips jobs that are already running, so `docker compose up` can be re-run.
set -euo pipefail

REST=http://jobmanager:8081

until wget -qO- "$REST/overview" >/dev/null 2>&1; do
  echo "waiting for Flink JobManager..."; sleep 2
done

is_running() {
  wget -qO- "$REST/jobs/overview" \
    | grep -Eq "\"name\":\"$1\"[^}]*\"state\":\"(RUNNING|CREATED|INITIALIZING|RESTARTING)\""
}

if is_running "fluss-tiering"; then
  echo "tiering service already running, skipping"
else
  echo "submitting Fluss -> Iceberg tiering service"
  /opt/flink/bin/flink run -d -m jobmanager:8081 -Dpipeline.name=fluss-tiering \
    /opt/flink/opt/fluss-flink-tiering-1.0.0.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type hive \
    --datalake.iceberg.uri thrift://metastore:9083 \
    --datalake.iceberg.warehouse "s3://fluss-demo/datalakehouse" \
    --datalake.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO \
    --datalake.iceberg.s3.endpoint "http://rustfs:9000" \
    --datalake.iceberg.s3.access-key-id "$S3_ACCESS_KEY" \
    --datalake.iceberg.s3.secret-access-key "$S3_SECRET_KEY" \
    --datalake.iceberg.s3.path-style-access true \
    --datalake.iceberg.client.region us-east-1
fi

if is_running "cdc"; then
  echo "CDC pipeline already running, skipping"
else
  echo "submitting Postgres -> Fluss CDC pipeline"
  bash /opt/sql-client.sh -f /tmp/sql/pipeline.sql
fi

echo "done. Flink UI: http://localhost:8081"
