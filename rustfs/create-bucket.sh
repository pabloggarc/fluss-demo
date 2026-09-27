#!/bin/sh
until rc alias set rustfs "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"; do
  echo "Waiting for RustFS..."
  sleep 1
done

rc mb --ignore-existing "rustfs/$BUCKET"
