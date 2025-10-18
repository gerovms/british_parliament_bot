#!/usr/bin/env bash

host="$1"
port="$2"
shift 2
timeout=30

while ! nc -z "$host" "$port"; do
  echo "Waiting for $host:$port..."
  sleep 1
  timeout=$((timeout-1))
  if [ $timeout -le 0 ]; then
    echo "Timeout waiting for $host:$port"
    exit 1
  fi
done

exec "$@"