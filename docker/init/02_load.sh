#!/usr/bin/env bash
set -euo pipefail

echo "Loading CSV files from /data into public.mock_data_raw..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'mock_data_raw'
  ) THEN
    RAISE EXCEPTION 'Table public.mock_data_raw does not exist';
  END IF;
END $$;
SQL

shopt -s nullglob
files=(/data/*.csv)
if [ "${#files[@]}" -eq 0 ]; then
  echo "No CSV files found at /data/*.csv"
  exit 1
fi

for f in "${files[@]}"; do
  echo " - importing: $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
\copy public.mock_data_raw FROM '$f' WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"')
SQL
done

echo "Done. Row count:"
psql -tA --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT count(*) FROM public.mock_data_raw;"
