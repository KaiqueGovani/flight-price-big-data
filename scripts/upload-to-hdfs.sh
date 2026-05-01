#!/usr/bin/env bash
# Upload local CSV files to HDFS. Idempotent and safe to run multiple times.
# Usage: docker compose exec namenode /scripts/upload-to-hdfs.sh
set -euo pipefail

HDFS_DIR="/data"
LOCAL_DIR="/data"

echo "Waiting for HDFS NameNode to leave safe mode..."
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  sleep 5
done
echo "NameNode is ready."

hdfs dfs -mkdir -p "$HDFS_DIR"

for f in "$LOCAL_DIR"/*.csv; do
  [ -f "$f" ] || continue
  fname=$(basename "$f")
  if hdfs dfs -test -e "$HDFS_DIR/$fname" 2>/dev/null; then
    echo "$fname already in HDFS, skipping."
  else
    echo "Uploading $fname ..."
    hdfs dfs -put "$f" "$HDFS_DIR/"
  fi
done

echo
echo "HDFS $HDFS_DIR contents:"
hdfs dfs -ls "$HDFS_DIR"
