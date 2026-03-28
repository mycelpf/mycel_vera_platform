-- Create mycel_vera_platform roles
-- Runs as: mycel_app_db_admin
-- Phase 1, Step 1

-- ============================================================
-- Owner role (non-login) — full access, owns this schema
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mycel_vera_platform_role') THEN
    CREATE ROLE mycel_vera_platform_role NOLOGIN;
  END IF;
END $$;

ALTER SCHEMA mycel_vera_platform OWNER TO mycel_vera_platform_role;
GRANT ALL PRIVILEGES ON SCHEMA mycel_vera_platform TO mycel_vera_platform_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA mycel_vera_platform TO mycel_vera_platform_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA mycel_vera_platform TO mycel_vera_platform_role;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA mycel_vera_platform TO mycel_vera_platform_role;

-- Future objects inherit full privileges for owner
ALTER DEFAULT PRIVILEGES IN SCHEMA mycel_vera_platform
  GRANT ALL PRIVILEGES ON TABLES TO mycel_vera_platform_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA mycel_vera_platform
  GRANT ALL PRIVILEGES ON SEQUENCES TO mycel_vera_platform_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA mycel_vera_platform
  GRANT ALL PRIVILEGES ON FUNCTIONS TO mycel_vera_platform_role;

-- ============================================================
-- Reader role (non-login) — SELECT only, for downstream modules
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mycel_vera_platform_reader_role') THEN
    CREATE ROLE mycel_vera_platform_reader_role NOLOGIN;
  END IF;
END $$;

GRANT USAGE ON SCHEMA mycel_vera_platform TO mycel_vera_platform_reader_role;
GRANT SELECT ON ALL TABLES IN SCHEMA mycel_vera_platform TO mycel_vera_platform_reader_role;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA mycel_vera_platform TO mycel_vera_platform_reader_role;

-- Future tables/sequences are also readable
ALTER DEFAULT PRIVILEGES IN SCHEMA mycel_vera_platform
  GRANT SELECT ON TABLES TO mycel_vera_platform_reader_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA mycel_vera_platform
  GRANT SELECT ON SEQUENCES TO mycel_vera_platform_reader_role;
