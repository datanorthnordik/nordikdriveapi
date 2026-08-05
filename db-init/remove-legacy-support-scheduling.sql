-- One-time destructive migration for the superseded support-call scheduler.
--
-- Run this script deliberately, after exporting any legacy support-call data,
-- and before applying db-init/init.sql from the redesigned implementation.
-- It removes only the old denormalized scheduling tables; generic
-- support_requests, support_schedule_settings, and support_team_members stay.
BEGIN;

DROP TABLE IF EXISTS support_call_requests CASCADE;
DROP TABLE IF EXISTS support_daily_assignments CASCADE;
DROP TABLE IF EXISTS support_staff_unavailabilities CASCADE;

COMMIT;
