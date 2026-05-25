IF OBJECT_ID('VW_SANCTIONS_DAILY_INSIGHTS', 'V') IS NOT NULL
    DROP VIEW VW_SANCTIONS_DAILY_INSIGHTS;

IF OBJECT_ID('SANCTIONS_DELTA_LOG', 'U') IS NOT NULL
    DROP TABLE SANCTIONS_DELTA_LOG;

IF OBJECT_ID('SANCTIONS_ENTITY', 'U') IS NOT NULL
    DROP TABLE SANCTIONS_ENTITY;

IF OBJECT_ID('SANCTIONS_RUN', 'U') IS NOT NULL
    DROP TABLE SANCTIONS_RUN;

CREATE TABLE SANCTIONS_RUN (
    id                    BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_code           NVARCHAR(20)   NOT NULL,
    current_file_name     NVARCHAR(500)  NOT NULL,
    previous_file_name    NVARCHAR(500)  NULL,
    parser_used           NVARCHAR(100)  NULL,
    storage_mode          NVARCHAR(20)   NOT NULL DEFAULT 'LOCAL',
    run_type              NVARCHAR(20)   NOT NULL,
    triggered_by          NVARCHAR(200)  NULL,
    run_status            NVARCHAR(20)   NOT NULL DEFAULT 'IN_PROGRESS',
    error_message         NVARCHAR(MAX)  NULL,
    skipped_count         INT            NOT NULL DEFAULT 0,
    added_count           INT            NOT NULL DEFAULT 0,
    removed_count         INT            NOT NULL DEFAULT 0,
    updated_count         INT            NOT NULL DEFAULT 0,
    total_current_entries INT            NULL,
    started_at            DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    completed_at          DATETIME2      NULL,
    duration_ms           AS (DATEDIFF(MILLISECOND, started_at, completed_at)),
    created_at            DATETIME2      NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_RUN_SOURCE  ON SANCTIONS_RUN (source_code, started_at DESC);
CREATE INDEX IX_RUN_TYPE    ON SANCTIONS_RUN (run_type);
CREATE INDEX IX_RUN_STATUS  ON SANCTIONS_RUN (run_status);

CREATE TABLE SANCTIONS_ENTITY (
    id               BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_code      NVARCHAR(20)   NOT NULL,
    business_key     NVARCHAR(500)  NOT NULL,
    source_ref       NVARCHAR(200)  NULL,
    entity_type      NVARCHAR(50)   NULL,
    primary_name     NVARCHAR(500)  NULL,
    nationality      NVARCHAR(200)  NULL,
    date_of_birth    NVARCHAR(50)   NULL,
    remarks          NVARCHAR(MAX)  NULL,
    is_active        BIT            NOT NULL DEFAULT 1,
    change_type      NVARCHAR(10)   NULL,
    canonical_hash   NVARCHAR(64)   NULL,
    last_seen_run_id BIGINT         NULL REFERENCES SANCTIONS_RUN(id),
    last_seen_at     DATETIME2      NULL,
    created_at       DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    updated_at       DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_ENTITY_SOURCE_KEY UNIQUE (source_code, business_key)
);

CREATE INDEX IX_ENTITY_NAME   ON SANCTIONS_ENTITY (primary_name);
CREATE INDEX IX_ENTITY_ACTIVE ON SANCTIONS_ENTITY (is_active, source_code);
CREATE INDEX IX_ENTITY_DOB    ON SANCTIONS_ENTITY (date_of_birth);
CREATE INDEX IX_ENTITY_NAT    ON SANCTIONS_ENTITY (nationality);
CREATE INDEX IX_ENTITY_REF    ON SANCTIONS_ENTITY (source_ref);

CREATE TABLE SANCTIONS_DELTA_LOG (
    id                    BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id                BIGINT        NOT NULL REFERENCES SANCTIONS_RUN(id),
    source_code           NVARCHAR(20)  NOT NULL,
    change_type           NVARCHAR(10)  NOT NULL,
    business_key          NVARCHAR(500) NOT NULL,
    entity_id             BIGINT        NULL REFERENCES SANCTIONS_ENTITY(id),
    previous_payload_json NVARCHAR(MAX) NULL,
    current_payload_json  NVARCHAR(MAX) NULL,
    field_changes_json    NVARCHAR(MAX) NULL,
    created_at            DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_DELTA_RUN        ON SANCTIONS_DELTA_LOG (run_id);
CREATE INDEX IX_DELTA_SOURCE_KEY ON SANCTIONS_DELTA_LOG (source_code, business_key);
CREATE INDEX IX_DELTA_TYPE       ON SANCTIONS_DELTA_LOG (change_type);

CREATE OR ALTER VIEW VW_SANCTIONS_DAILY_INSIGHTS AS
SELECT
    CAST(started_at AS DATE)   AS processing_date,
    source_code,
    run_type,
    COUNT(*)                   AS total_runs,
    SUM(added_count)           AS total_added,
    SUM(removed_count)         AS total_removed,
    SUM(updated_count)         AS total_updated,
    MAX(total_current_entries) AS total_current_entries
FROM SANCTIONS_RUN
WHERE run_status IN ('COMPLETED', 'PARTIAL_SUCCESS')
GROUP BY CAST(started_at AS DATE), source_code, run_type;
