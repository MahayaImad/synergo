-- Initialisation des schémas Synergo
CREATE SCHEMA IF NOT EXISTS synergo_core;
CREATE SCHEMA IF NOT EXISTS synergo_sync;
CREATE SCHEMA IF NOT EXISTS synergo_analytics;

-- Extensions nécessaires
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Utilisateur pour l'application
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'synergo_app') THEN
      CREATE ROLE synergo_app LOGIN PASSWORD 'app_password_2024';
   END IF;
END
$$;

-- Permissions
GRANT USAGE ON SCHEMA synergo_core TO synergo_app;
GRANT USAGE ON SCHEMA synergo_sync TO synergo_app;
GRANT USAGE ON SCHEMA synergo_analytics TO synergo_app;

GRANT CREATE ON SCHEMA synergo_core TO synergo_app;
GRANT CREATE ON SCHEMA synergo_sync TO synergo_app;
GRANT CREATE ON SCHEMA synergo_analytics TO synergo_app;