CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'iceberg.s3.access-key-id' = '${S3_ACCESS_KEY}',
    'iceberg.s3.secret-access-key' = '${S3_SECRET_KEY}'
);

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://metastore:9083',
    'warehouse' = 's3://fluss-demo/datalakehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://rustfs:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = '${S3_ACCESS_KEY}',
    's3.secret-access-key' = '${S3_SECRET_KEY}',
    'client.region' = 'us-east-1'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS bronze;
USE bronze;

SET 'sql-client.execution.result-mode' = 'tableau';
