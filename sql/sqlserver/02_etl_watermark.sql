USE dwh;
GO

-- Estado de la carga incremental por entidad.
-- watermark_value es el máximo updated_at confirmado (UTC).
IF OBJECT_ID(N'etl.watermark', N'U') IS NULL
CREATE TABLE etl.watermark (
    entity_name     NVARCHAR(100) NOT NULL,
    watermark_value DATETIME2(6)  NOT NULL,
    last_run_at     DATETIME2(3)  NOT NULL CONSTRAINT df_watermark_last_run DEFAULT SYSUTCDATETIME(),
    rows_extracted  INT           NOT NULL CONSTRAINT df_watermark_rows DEFAULT 0,
    last_status     NVARCHAR(20)  NOT NULL CONSTRAINT df_watermark_status DEFAULT N'success',
    CONSTRAINT pk_watermark PRIMARY KEY CLUSTERED (entity_name)
);
GO
