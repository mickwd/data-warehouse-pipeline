-- =============================================================================
-- Base de datos del Data Warehouse y esquemas por capa
--   landing : copia efímera del último lote extraído (truncate + insert)
--   raw     : estado actual de cada entidad de origen (MERGE idempotente)
--   staging : vistas de limpieza de dbt
--   dw      : modelo dimensional (esquema estrella, Kimball)
--   etl     : metadata operativa (watermarks, auditoría)
-- =============================================================================
IF DB_ID(N'dwh') IS NULL
BEGIN
    CREATE DATABASE dwh;
END
GO

-- Snapshot isolation para que los reportes no se bloqueen con las cargas
ALTER DATABASE dwh SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
GO

USE dwh;
GO

IF SCHEMA_ID(N'landing') IS NULL EXEC (N'CREATE SCHEMA landing');
IF SCHEMA_ID(N'raw')     IS NULL EXEC (N'CREATE SCHEMA raw');
IF SCHEMA_ID(N'staging') IS NULL EXEC (N'CREATE SCHEMA staging');
IF SCHEMA_ID(N'dw')      IS NULL EXEC (N'CREATE SCHEMA dw');
IF SCHEMA_ID(N'etl')     IS NULL EXEC (N'CREATE SCHEMA etl');
GO
