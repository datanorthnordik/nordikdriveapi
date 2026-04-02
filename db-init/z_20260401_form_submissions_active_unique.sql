ALTER TABLE form_submissions
    DROP CONSTRAINT IF EXISTS uq_form_submissions_file_row_form;

DROP INDEX IF EXISTS uq_form_submissions_file_row_form_active;

CREATE UNIQUE INDEX uq_form_submissions_file_row_form_active
    ON form_submissions (file_id, row_id, form_key)
    WHERE status <> 'rejected';
