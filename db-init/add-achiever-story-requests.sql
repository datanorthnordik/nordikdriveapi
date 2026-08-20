-- Apply this once to an existing database after the base schema in init.sql
-- has already been created. New environments receive the same final schema
-- directly from init.sql.

-- Mark each file edit request by its purpose so story submissions can be
-- displayed and reviewed separately from ordinary record edits.
ALTER TABLE file_edit_request
ADD COLUMN IF NOT EXISTS request_type VARCHAR(30) NOT NULL DEFAULT 'edit';

UPDATE file_edit_request
SET request_type = 'edit'
WHERE request_type IS NULL OR BTRIM(request_type) = '';

ALTER TABLE file_edit_request
DROP CONSTRAINT IF EXISTS file_edit_request_type_check;

ALTER TABLE file_edit_request
ADD CONSTRAINT file_edit_request_type_check
CHECK (request_type IN ('edit', 'achiever_story'));

CREATE INDEX IF NOT EXISTS idx_file_edit_request_type_status_created
    ON file_edit_request(request_type, status, created_at, request_id);

-- Link a submitted story to its review request and retain its review outcome.
ALTER TABLE file_row_achiever_stories
ADD COLUMN IF NOT EXISTS request_id INT NULL
    REFERENCES file_edit_request(request_id) ON DELETE SET NULL;

ALTER TABLE file_row_achiever_stories
ADD COLUMN IF NOT EXISTS reviewed_by INT NULL
    REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE file_row_achiever_stories
ADD COLUMN IF NOT EXISTS reviewer_comment TEXT NOT NULL DEFAULT '';

ALTER TABLE file_row_achiever_stories
ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMP NULL;

-- The old unconditional key prevented more than one user-submitted story for
-- a row because manual stories have blank import-source fields. Preserve the
-- uniqueness rule for imported spreadsheet rows only.
ALTER TABLE file_row_achiever_stories
DROP CONSTRAINT IF EXISTS uq_file_row_achiever_story_source;

DROP INDEX IF EXISTS uq_file_row_achiever_story_source;

CREATE UNIQUE INDEX IF NOT EXISTS uq_file_row_achiever_story_source
    ON file_row_achiever_stories(file_id, source_workbook, source_sheet, row_id, source_row)
    WHERE source_workbook <> '' OR source_sheet <> '' OR source_row <> 0;

CREATE INDEX IF NOT EXISTS idx_achiever_story_request
    ON file_row_achiever_stories(request_id);
