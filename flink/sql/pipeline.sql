SET 'pipeline.name' = 'cdc';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.dml-sync' = 'false';

CREATE TABLE IF NOT EXISTS downloads (
    download_id  STRING,
    user_id      STRING,
    ip           STRING,
    country      STRING,
    tool         STRING,
    version      STRING,
    os           STRING,
    channel      STRING,
    status       STRING,
    size_bytes   BIGINT,
    duration_ms  INT,
    created_at   TIMESTAMP(3),
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (download_id) NOT ENFORCED
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

CREATE TEMPORARY TABLE pg_downloads (
    download_id  STRING,
    user_id      STRING,
    ip           STRING,
    country      STRING,
    tool         STRING,
    version      STRING,
    os           STRING,
    channel      STRING,
    status       STRING,
    size_bytes   BIGINT,
    duration_ms  INT,
    created_at   TIMESTAMP(3),
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (download_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'source-db',
    'port' = '5432',
    'username' = '${POSTGRES_USER}',
    'password' = '${POSTGRES_PASSWORD}',
    'database-name' = 'demo',
    'schema-name' = 'public',
    'table-name' = 'downloads',
    'slot.name' = 'flink_downloads',
    'decoding.plugin.name' = 'pgoutput',
    'debezium.publication.name' = 'flink_pub',
    'changelog-mode' = 'upsert',
    'scan.incremental.snapshot.enabled' = 'true'
);

INSERT INTO downloads SELECT * FROM pg_downloads;
