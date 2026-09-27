#!/bin/bash
# The image always runs `schematool -initSchema`, which fails if the embedded Derby
# database already exists (e.g. after `docker compose stop/start`).
if [ -d /opt/hive/metastore_db ]; then
  export IS_RESUME=true
fi
exec /entrypoint.sh
