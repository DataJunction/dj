#!/bin/bash
psql -U dj -d dj <<EOF
DO \$\$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'readonly_user'
   ) THEN
      CREATE ROLE readonly_user LOGIN PASSWORD 'readonly_pass';
   END IF;
END
\$\$;

GRANT CONNECT ON DATABASE dj TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;
EOF
