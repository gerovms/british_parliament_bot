#!/bin/sh
set -e

host="$1"
shift

# Важно! Устанавливаем пароль для pg_isready
export PGPASSWORD="${POSTGRES_PASSWORD}"

count=0
until pg_isready -h "$host" -U "$POSTGRES_USER" > /dev/null 2>&1; do
  echo "Waiting for postgres at $host..."
  count=$((count + 1))
  if [ $count -gt 60 ]; then   # максимум 2 минуты
    echo "Postgres did not become ready in 120 seconds, exiting"
    exit 1
  fi
  sleep 2
done

echo "Postgres is ready, starting the command..."
exec "$@"
