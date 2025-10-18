#!/bin/sh
# wait-for-postgres.sh
set -e
host="$1"
shift
until pg_isready -h "$host" -U "${POSTGRES_USER}" > /dev/null 2>&1; do
  echo "Waiting for postgres at $host..."
  sleep 2
done
exec "$@"