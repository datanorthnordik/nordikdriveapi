-- Manual one-time migration for enabling Zoom on existing support-call data.
-- The application does not execute this script automatically.

BEGIN;

ALTER TABLE support_calls
    ADD COLUMN IF NOT EXISTS zoom_meeting_id VARCHAR(32) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS zoom_join_url TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS zoom_passcode VARCHAR(16) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS zoom_host_email VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS zoom_sync_status VARCHAR(32) NOT NULL DEFAULT 'not_requested',
    ADD COLUMN IF NOT EXISTS zoom_sync_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS zoom_synced_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_support_calls_zoom_sync
    ON support_calls(zoom_sync_status, scheduled_start_time);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'support_calls_zoom_sync_status_check'
          AND conrelid = 'support_calls'::regclass
    ) THEN
        ALTER TABLE support_calls
            ADD CONSTRAINT support_calls_zoom_sync_status_check
            CHECK (zoom_sync_status IN ('not_requested', 'pending', 'synced', 'failed', 'deleted'));
    END IF;
END $$;

COMMIT;
