-- =============================================================================
-- V2: Enhance sanctions_run table
-- Run this script if you already applied schema.sql (V1).
-- Safe to run multiple times — each ALTER uses IF NOT EXISTS guard.
-- =============================================================================

-- File integrity / traceability columns
IF NOT EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID('sanctions_run') AND name = 'file_checksum')
    ALTER TABLE sanctions_run ADD file_checksum NVARCHAR(64) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID('sanctions_run') AND name = 'parser_used')
    ALTER TABLE sanctions_run ADD parser_used NVARCHAR(100) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID('sanctions_run') AND name = 'storage_mode')
    ALTER TABLE sanctions_run ADD storage_mode NVARCHAR(20) NOT NULL DEFAULT 'LOCAL';

-- Richer run status lifecycle
IF NOT EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID('sanctions_run') AND name = 'skipped_count')
    ALTER TABLE sanctions_run ADD skipped_count INT NOT NULL DEFAULT 0;

-- Migrate existing SUCCESS rows to COMPLETED so queries remain consistent.
-- Old rows with status='SUCCESS' will now be treated as COMPLETED.
UPDATE sanctions_run
SET    run_status = 'COMPLETED'
WHERE  run_status = 'SUCCESS';

-- New default for run_status is IN_PROGRESS (set at run start, updated at end).
-- SQL Server does not support ALTER COLUMN DEFAULT without dropping the old constraint first.
DECLARE @constraintName NVARCHAR(200);
SELECT @constraintName = dc.name
FROM   sys.default_constraints dc
JOIN   sys.columns             c  ON dc.parent_object_id = c.object_id
                                  AND dc.parent_column_id = c.column_id
WHERE  c.object_id = OBJECT_ID('sanctions_run')
AND    c.name      = 'run_status';

IF @constraintName IS NOT NULL
    EXEC('ALTER TABLE sanctions_run DROP CONSTRAINT ' + @constraintName);

ALTER TABLE sanctions_run
    ADD CONSTRAINT DF_sanctions_run_status DEFAULT 'IN_PROGRESS' FOR run_status;
