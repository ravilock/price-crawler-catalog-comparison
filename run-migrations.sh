#!/bin/sh
set -eu
if [ -z "${DATABASE_URL:-}" ]; then
  echo 'catalog-comparison migration requires DATABASE_URL' >&2
  exit 1
fi
exec /usr/local/bin/migrate -path /migrations -database "$DATABASE_URL" "$@"
