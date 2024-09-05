DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_catalog.pg_database
      WHERE  datname = 'djqs') THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE djqs');
   END IF;
END
$do$;